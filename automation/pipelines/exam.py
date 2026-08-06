from __future__ import annotations

from datetime import datetime
import html
import json
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

from playwright.sync_api import Page, sync_playwright

from automation_core.clients import postgres_connect, send_telegram
from automation_core.connections import VaultConnections

AppointmentKey = tuple[str, datetime]
APPOINTMENT_TIMEZONE = ZoneInfo("Europe/Zurich")


class AppointmentChanges(NamedTuple):
    unavailable: set[AppointmentKey]
    newly_seen: set[AppointmentKey]
    newly_available: set[AppointmentKey]
    unnotified_available: set[AppointmentKey]


def _parse_appointment_datetime(appointment: dict[str, Any]) -> datetime:
    value = datetime.strptime(
        f"{appointment['date']} {appointment['time']}", "%d.%m.%Y %H:%M"
    )
    return value.replace(tzinfo=APPOINTMENT_TIMEZONE)


def _appointment_key(location: str, value: datetime) -> AppointmentKey:
    return location, value if value.tzinfo else value.replace(
        tzinfo=APPOINTMENT_TIMEZONE
    )


def _split_appointment_changes(
    scraped: set[AppointmentKey],
    records: list[tuple[str, datetime, str, datetime | None]],
) -> AppointmentChanges:
    available = {
        _appointment_key(a, b) for a, b, status, _ in records if status == "available"
    }
    known = {_appointment_key(a, b) for a, b, _, _ in records}
    notified = {_appointment_key(a, b) for a, b, _, at in records if at is not None}
    return AppointmentChanges(
        available - scraped, scraped - known, scraped - available, scraped - notified
    )


def render_appointments_for_telegram(appointments: list[dict[str, Any]]) -> str:
    ordered = sorted(appointments, key=_parse_appointment_datetime)
    lines = ["<b>Available exam dates</b>", ""]
    for appointment in ordered:
        lines.append(
            f"- <b>{html.escape(appointment['location'])}</b>: {html.escape(appointment['date'])} at {html.escape(appointment['time'])}"
        )
    return "\n".join(lines)


def scrape_current_week(
    page: Page, location: str, selector: str
) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    for day in page.locator(selector).all():
        if day.locator('p:has-text("Keine Termine frei")').count():
            continue
        for slot in day.locator('div[id^="listeHeures"] button').all():
            found.append(
                {
                    "location": location,
                    "date": day.locator("h3").inner_text().strip(),
                    "time": slot.inner_text().strip(),
                }
            )
    return found


def scrape(vault: VaultConnections) -> list[dict[str, str]]:
    connection = vault.get("stva")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91 Safari/537.36"
            )
            page.goto(connection.host)
            page.locator("#birthday input").fill(connection.login)
            page.locator("#candidateId").fill(connection.password)
            page.locator('button.dispoButton[type="submit"]').click()
            page.locator('a:has-text("Auswählen")').click()
            page.locator("#desktop.dateTimeSelector").wait_for(
                state="visible", timeout=30000
            )
            dropdown = page.locator("select#lieu")
            appointments: list[dict[str, str]] = []
            for location in [
                item.inner_text() for item in dropdown.locator("option").all()
            ]:
                dropdown.select_option(label=location)
                page.locator("dw-week-list div#jour").first.wait_for(
                    state="visible", timeout=15000
                )
                while True:
                    appointments.extend(
                        scrape_current_week(page, location, "dw-week-list div#jour")
                    )
                    button = page.locator("div#right button")
                    if button.is_disabled():
                        break
                    button.click()
                    page.locator("dw-week-list div#jour").first.wait_for(
                        state="visible", timeout=15000
                    )
            return appointments
        finally:
            browser.close()


def run(vault: VaultConnections) -> None:
    now = datetime.now(APPOINTMENT_TIMEZONE)
    appointments = scrape(vault)
    with postgres_connect(vault.get("data")) as database:
        with database.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS exam_appointment (id SERIAL PRIMARY KEY, location VARCHAR(255) NOT NULL, appointment_datetime TIMESTAMPTZ NOT NULL, status VARCHAR(20) NOT NULL CHECK (status IN ('available','unavailable')), first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), last_seen_at TIMESTAMPTZ, notified_at TIMESTAMPTZ, became_unavailable_at TIMESTAMPTZ, UNIQUE(location, appointment_datetime)); CREATE INDEX IF NOT EXISTS idx_exam_appointment_status ON exam_appointment(status); CREATE INDEX IF NOT EXISTS idx_exam_appointment_datetime ON exam_appointment(appointment_datetime); CREATE TABLE IF NOT EXISTS exam_log (id SERIAL PRIMARY KEY, run_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), run_finished_at TIMESTAMPTZ, status VARCHAR(20) NOT NULL CHECK(status IN ('success','failed')), appointments_found INTEGER, new_appointments_added INTEGER, appointments_marked_unavailable INTEGER, error_message TEXT, raw_response_json JSONB)"""
            )
            cursor.execute(
                "INSERT INTO exam_log(status) VALUES ('success') RETURNING id"
            )
            log_id = cursor.fetchone()[0]
            scraped = {
                (item["location"], _parse_appointment_datetime(item))
                for item in appointments
            }
            cursor.execute(
                "SELECT location, appointment_datetime, status, notified_at FROM exam_appointment"
            )
            changes = _split_appointment_changes(scraped, cursor.fetchall())
            cursor.executemany(
                "UPDATE exam_appointment SET status='unavailable', became_unavailable_at=%s WHERE location=%s AND appointment_datetime=%s AND status='available'",
                [(now, *item) for item in changes.unavailable],
            )
            cursor.executemany(
                """INSERT INTO exam_appointment(location, appointment_datetime, status, first_seen_at, last_seen_at) VALUES (%s,%s,'available',%s,%s) ON CONFLICT(location,appointment_datetime) DO UPDATE SET status='available',last_seen_at=EXCLUDED.last_seen_at,became_unavailable_at=NULL""",
                [(*item, now, now) for item in changes.newly_available],
            )
            cursor.executemany(
                "UPDATE exam_appointment SET last_seen_at=%s WHERE location=%s AND appointment_datetime=%s",
                [(now, *item) for item in scraped - changes.newly_available],
            )
            cursor.execute(
                "UPDATE exam_log SET run_finished_at=%s,appointments_found=%s,new_appointments_added=%s,appointments_marked_unavailable=%s,raw_response_json=%s WHERE id=%s",
                (
                    now,
                    len(appointments),
                    len(changes.newly_seen),
                    len(changes.unavailable),
                    json.dumps(appointments),
                    log_id,
                ),
            )
        database.commit()
        pending = [
            {
                "location": location,
                "date": value.strftime("%d.%m.%Y"),
                "time": value.strftime("%H:%M"),
            }
            for location, value in changes.unnotified_available
        ]
        if pending:
            send_telegram(
                vault.get("telegram_default"),
                render_appointments_for_telegram(pending),
                parse_mode="HTML",
            )
            with database.cursor() as cursor:
                cursor.executemany(
                    "UPDATE exam_appointment SET notified_at=%s WHERE location=%s AND appointment_datetime=%s",
                    [(now, *item) for item in changes.unnotified_available],
                )
            database.commit()
