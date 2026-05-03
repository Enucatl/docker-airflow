from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from exam import (
    dag,
    render_appointments_for_telegram,
    _parse_appointment_datetime,
    _split_appointment_changes,
)


def test_render_appointments_for_telegram_sorts_and_escapes() -> None:
    appointments = [
        {
            "location": "Zurich",
            "date": "02.09.2026",
            "time": "11:15",
        },
        {
            "location": "A&B",
            "date": "01.09.2026",
            "time": "09:00",
        },
    ]

    message = render_appointments_for_telegram(appointments)

    assert (
        message == "<b>Available exam dates</b>\n\n"
        "- <b>A&amp;B</b>: 01.09.2026 at 09:00\n"
        "- <b>Zurich</b>: 02.09.2026 at 11:15"
    )


def test_exam_dag_exposes_telegram_notification_task() -> None:
    format_appointments = dag.get_task("format_appointments_for_telegram")
    send_exam_dates = dag.get_task("send_exam_dates")
    mark_notified = dag.get_task("mark_appointments_notified")

    assert dag.schedule == "@hourly"
    assert set(dag.task_ids) >= {
        "extract_appointments",
        "format_appointments_for_telegram",
        "send_exam_dates",
    }
    assert format_appointments._pre_execute_hook is not None
    assert send_exam_dates.telegram_conn_id == "telegram_default"
    assert format_appointments.upstream_task_ids >= {"update_database"}
    assert send_exam_dates.upstream_task_ids == {"format_appointments_for_telegram"}
    assert mark_notified.upstream_task_ids >= {"send_exam_dates"}


def test_parse_appointment_datetime_returns_aware_timestamp() -> None:
    appointment_datetime = _parse_appointment_datetime(
        {"location": "Albisgütli", "date": "22.05.2026", "time": "07:00"}
    )

    assert appointment_datetime == datetime(
        2026, 5, 22, 7, 0, tzinfo=ZoneInfo("Europe/Zurich")
    )


def test_split_appointment_changes_only_flags_never_seen_for_notification() -> None:
    timezone = ZoneInfo("Europe/Zurich")
    notified_at = datetime(2026, 8, 1, 12, 0, tzinfo=timezone)
    new_appointment = ("Zurich", datetime(2026, 9, 1, 9, 0, tzinfo=timezone))
    reopened_appointment = ("Zurich", datetime(2026, 9, 2, 10, 0, tzinfo=timezone))
    still_available_appointment = (
        "Winterthur",
        datetime(2026, 9, 3, 11, 0, tzinfo=timezone),
    )
    now_unavailable_appointment = (
        "Uster",
        datetime(2026, 9, 4, 12, 0, tzinfo=timezone),
    )

    changes = _split_appointment_changes(
        {
            new_appointment,
            reopened_appointment,
            still_available_appointment,
        },
        [
            (*reopened_appointment, "unavailable", notified_at),
            (*still_available_appointment, "available", notified_at),
            (*now_unavailable_appointment, "available", notified_at),
        ],
    )

    assert changes.newly_seen == {new_appointment}
    assert changes.newly_available == {new_appointment, reopened_appointment}
    assert changes.unavailable == {now_unavailable_appointment}
    assert changes.unnotified_available == {new_appointment}
