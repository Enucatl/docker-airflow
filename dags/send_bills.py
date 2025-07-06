from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator

HTTP_CONN_ID = "bills_api"

# Define default arguments for the DAG
default_args = {
    "retries": 0,
    "depends_on_past": False,
    "email_on_retry": False,
    "email_on_failure": True,
}

with DAG(
    dag_id="billing",
    start_date=datetime(2025, 7, 6),
    schedule="@daily",
    default_args=default_args,
    description="A DAG to run daily billing tasks against the Django API.",
    catchup=False,
    tags=[],
) as dag:
    # Task to generate recurring bills
    generate_recurring_bills = HttpOperator(
        task_id="generate_recurring_bills",
        http_conn_id=HTTP_CONN_ID,
        endpoint="api/generate-recurring-bills/",
        method="POST",
        log_response=True,
    )

    # Task to send notifications for newly created pending bills
    send_pending_bills = HttpOperator(
        task_id="send_pending_bills",
        http_conn_id=HTTP_CONN_ID,
        endpoint="api/send-pending-bills/",
        method="POST",
        log_response=True,
    )

    # Task to mark bills that are now overdue
    mark_overdue_bills = HttpOperator(
        task_id="mark_overdue_bills",
        http_conn_id=HTTP_CONN_ID,
        endpoint="api/mark-overdue-bills/",
        method="POST",
        log_response=True,
    )

    # Task to send notifications for overdue bills
    notify_overdue_bills = HttpOperator(
        task_id="notify_overdue_bills",
        http_conn_id=HTTP_CONN_ID,
        endpoint="api/notify-overdue-bills/",
        method="POST",
        log_response=True,
    )

    (
        generate_recurring_bills
        >> send_pending_bills
        >> mark_overdue_bills
        >> notify_overdue_bills
    )
