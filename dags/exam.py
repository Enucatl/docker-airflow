from datetime import datetime
import json
import logging
from typing import List, Dict


from airflow import DAG
from airflow.decorators import task
from playwright.sync_api import sync_playwright, Page
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.defaults import default_args


logger = logging.getLogger(__name__)


def scrape_current_week(
    page: Page, location_name: str, day_containers_selector: str
) -> List[Dict]:
    """
    Helper function to scrape appointments from the currently visible week using a precise selector.
    """
    appointments_in_week = []
    day_containers = page.locator(day_containers_selector).all()

    for day_container in day_containers:
        # This check correctly identifies days with no free slots.
        if day_container.locator('p:has-text("Keine Termine frei")').count() > 0:
            continue

        # If we reach here, the day has available appointments.
        try:
            date = day_container.locator("h3").inner_text()

            # --- THE KEY FIX IS HERE ---
            # This selector is now based on the exact HTML of a day with appointments.
            time_slot_selector = 'div[id^="listeHeures"] button'

            time_slots = day_container.locator(time_slot_selector).all()

            if not time_slots:
                # This is a safeguard in case the 'Keine Termine frei' text is missing
                # but there are still no button elements.
                continue

            for slot in time_slots:
                appointment_time = slot.inner_text()
                appointments_in_week.append({
                    "location": location_name,
                    "date": date.strip(),
                    "time": appointment_time.strip(),
                })
        except Exception as e:
            logger.warning(
                f"Could not parse a day's appointments, even though it seemed to have slots. Error: {e}"
            )

    return appointments_in_week


with DAG(
    "exam",
    default_args=default_args,
    description="Scrape exam dates out of the Kanton Zurich website",
    schedule="@daily",
    start_date=datetime(2025, 8, 17),
) as dag:
    appointment_table_name = f"{dag.dag_id}_appointment"
    log_table_name = f"{dag.dag_id}_log"
    postgres_conn_id = "data"

    appointments_creation_sql = f"""
        CREATE TABLE IF NOT EXISTS {appointment_table_name} (
            id SERIAL PRIMARY KEY,
            location VARCHAR(255) NOT NULL,
            appointment_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
            status VARCHAR(20) NOT NULL CHECK (status IN ('available', 'unavailable')),
            first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMP WITH TIME ZONE,
            became_unavailable_at TIMESTAMP WITH TIME ZONE,
            -- This unique constraint is critical for our logic
            UNIQUE (location, appointment_datetime)
        );
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_{appointment_table_name}_status ON {appointment_table_name} (status);
        CREATE INDEX IF NOT EXISTS idx_{appointment_table_name}_datetime ON {appointment_table_name} (appointment_datetime);
    """

    scrape_logs_creation_sql = f"""
        CREATE TABLE IF NOT EXISTS {log_table_name} (
            id SERIAL PRIMARY KEY,
            run_started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            run_finished_at TIMESTAMP WITH TIME ZONE,
            status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed')),
            appointments_found INTEGER,
            new_appointments_added INTEGER,
            appointments_marked_unavailable INTEGER,
            error_message TEXT,
            raw_response_json JSONB -- Using JSONB is best practice in Postgres
        );
    """

    @task
    def create_table(create_sql: str, table_name: str) -> str:
        """Creates the appointments table if it doesn't exist."""
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        pg_hook.run(create_sql)
        logger.info(f"Table {table_name} is ready.")
        return table_name

    @task
    def extract_appointments():
        connection = BaseHook.get_connection("stva")
        # Scrape the webpage
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        print(connection.host)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=user_agent)
            username_selector = "#birthday input"
            password_selector = "#candidateId"
            submit_selector = 'button.dispoButton[type="submit"]'
            category_selector = 'a:has-text("Auswählen")'
            calendar_header_selector = "#desktop.dateTimeSelector"
            location_dropdown_selector = "select#lieu"
            next_week_button_selector = "div#right button"
            day_containers_selector = "dw-week-list div#jour"

            try:
                # Go to URL and wait for content to load
                page.goto(connection.host)
                page.locator(username_selector).fill(connection.login)
                page.locator(password_selector).fill(connection.get_password())
                page.locator(submit_selector).click()
                logger.info("submitting login...")
                category_link = page.locator(category_selector)
                category_link.wait_for(state="visible", timeout=30000)
                logger.info("clicking category link...")
                category_link.click()
                calendar_header = page.locator(calendar_header_selector)
                calendar_header.wait_for(state="visible", timeout=30000)
                logger.info("landed on the calendar page")

                location_dropdown = page.locator(location_dropdown_selector)
                all_location_options = location_dropdown.locator("option").all()
                location_names = [opt.inner_text() for opt in all_location_options]
                logger.info(f"Found locations: {location_names}")

                all_appointments = []
                # === PART 3: LOOP THROUGH EACH LOCATION AND SCRAPE ===
                for location in location_names:
                    logger.info(f"--- Processing location: {location} ---")
                    location_dropdown.select_option(label=location)
                    # Wait for the calendar to refresh after changing location.
                    # A short, fixed wait is often effective for SPAs after an action.
                    page.locator(day_containers_selector).first.wait_for(
                        state="visible", timeout=15000
                    )

                    # Loop through the weeks for this location
                    while True:
                        # Scrape appointments from the currently visible week
                        found_appointments = scrape_current_week(
                            page, location, day_containers_selector
                        )
                        if found_appointments:
                            logger.info(
                                f"Found {len(found_appointments)} appointments for {location} in the current week."
                            )
                            all_appointments.extend(found_appointments)

                        # Check the 'next week' button state and click if possible
                        next_button = page.locator(next_week_button_selector)
                        if next_button.is_disabled():
                            logger.info(
                                f"Next week button is disabled. End of calendar for {location}."
                            )
                            break  # Exit the while loop for this location

                        next_button.click()
                        # Wait for the next week's data to load
                        page.locator(day_containers_selector).first.wait_for(
                            state="visible", timeout=15000
                        )
                    break
            finally:
                browser.close()
        return all_appointments

    @task
    def update_database(
        scraped_appointments: list,
        appointment_table_name: str,
        log_table_name: str,
        data_interval_end: datetime,
    ):
        """
        Performs the 'diff' logic and updates the database statefully.
        """
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        # 1. Start the log entry and get its ID
        start_log_sql = f"""
        INSERT INTO {log_table_name} (status) VALUES ('success') RETURNING id;
        """
        log_id = pg_hook.get_first(start_log_sql)[0]
        logger.info(f"Started scrape log with ID: {log_id}")

        # 2. Prepare data for comparison
        # Convert scraped data to a set of (location, datetime_str) for easy lookup
        # We parse the date and time into a proper datetime object
        scraped_set = set()
        for appt in scraped_appointments:
            dt_str = f"{appt['date']} {appt['time']}"
            # DD.MM.YYYY HH:MI -> YYYY-MM-DD HH:MI:SS
            dt_obj = datetime.strptime(dt_str, "%d.%m.%Y %H:%M")
            scraped_set.add((appt["location"], dt_obj))

        # Get currently available appointments from the DB
        db_records = pg_hook.get_records(
            f"SELECT location, appointment_datetime FROM {appointment_table_name} WHERE status = 'available';"
        )
        db_set = set((rec[0], rec[1]) for rec in db_records)

        # 3. Find newly unavailable appointments
        unavailable_appointments = db_set - scraped_set
        if unavailable_appointments:
            logger.info(
                f"Found {len(unavailable_appointments)} appointments that are now unavailable."
            )
            for location, dt_obj in unavailable_appointments:
                pg_hook.run(
                    f"""
                    UPDATE {appointment_table_name}
                    SET status = 'unavailable', became_unavailable_at = %s
                    WHERE location = %s AND appointment_datetime = %s AND status = 'available';
                    """,
                    parameters=(data_interval_end, location, dt_obj),
                )

        # 4. Find new or re-opened appointments
        new_appointments = scraped_set - db_set
        if new_appointments:
            logger.info(f"Found {len(new_appointments)} new or re-opened appointments.")
            for location, dt_obj in new_appointments:
                # This "UPSERT" logic is key. It inserts a new appointment, but if a row
                # with the same location/datetime already exists (the UNIQUE constraint),
                # it updates the existing row instead.
                upsert_sql = f"""
                    INSERT INTO {appointment_table_name} (location, appointment_datetime, status, first_seen_at, last_seen_at)
                    VALUES (%s, %s, 'available', %s, %s)
                    ON CONFLICT (location, appointment_datetime)
                    DO UPDATE SET
                        status = 'available',
                        last_seen_at = EXCLUDED.last_seen_at, -- Use the value from the attempted insert
                    became_unavailable_at = NULL;
                """
            pg_hook.run(
                upsert_sql,
                parameters=(location, dt_obj, data_interval_end, data_interval_end),
            )

        # 5. Update last_seen_at for appointments that are still available
        still_available = scraped_set.intersection(db_set)
        if still_available:
            # We can do this in a single, efficient query
            # Create a list of tuples for the WHERE IN clause
            where_in_tuples = list(still_available)
            pg_hook.run(
                f"""
                UPDATE {appointment_table_name}
                SET last_seen_at = %s
                WHERE (location, appointment_datetime) IN %s;
                """,
                parameters=(data_interval_end, where_in_tuples),
            )

        # 6. Finalize the log entry with all the stats
        finish_log_sql = f"""
        UPDATE {log_table_name}
        SET
            run_finished_at = %s,
            appointments_found = %s,
            new_appointments_added = %s,
            appointments_marked_unavailable = %s,
            raw_response_json = %s
        WHERE id = %s;
        """
        pg_hook.run(
            finish_log_sql,
            parameters=(
                data_interval_end,
                len(scraped_appointments),
                len(new_appointments),
                len(unavailable_appointments),
                json.dumps(scraped_appointments),
                log_id,
            ),
        )
        logger.info("Database update complete and log finalized.")

    # --- Define the DAG Structure ---
    # Create tables in parallel

    create_appointments_table_task = create_table.override(
        task_id="create_appointments_table"
    )(create_sql=appointments_creation_sql, table_name=appointment_table_name)

    create_log_table_task = create_table.override(task_id="create_log_table")(
        create_sql=scrape_logs_creation_sql, table_name=log_table_name
    )

    # Run the main workflow after tables are ready
    scraped_data = extract_appointments()
    update_database(
        scraped_data,
        create_appointments_table_task,
        create_log_table_task,
    )
