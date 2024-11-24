from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator

from common.defaults import default_args

with DAG(
    "email",
    default_args=default_args,
    description="A DAG to test failure emails",
    schedule="@once",
    start_date=datetime(2024, 11, 22),
) as dag:

    @task.virtualenv(requirements=[])
    def failing_task():
        raise Exception("This task is meant to fail!")

    # Explicit email task
    send_test_email = EmailOperator(
        task_id="send_test_email",
        to=default_args["email"],
        subject="Airflow Test Email",
        html_content="This is a test email sent from Airflow EmailOperator.",
    )
    send_test_email >> failing_task()
