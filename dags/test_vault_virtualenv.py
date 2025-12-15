from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from airflow.sdk.bases.hook import BaseHook


default_args = {
    "depends_on_past": False,
    "retries": 0,
    "venv_cache_path": "/opt/airflow/venv",
}


with DAG(
    "test_vault_virtualenv",
    default_args=default_args,
    description="Test vault access for connections",
    schedule="@once",
    start_date=datetime(2025, 8, 17),
) as dag:

    @task
    def test_connection():
        connection = BaseHook.get_connection("stva")
        print(connection.host)

    @task.virtualenv(
        requirements=[],
    )
    def test_virtualenv(logical_date):
        from airflow.sdk.bases.hook import BaseHook

        connection = BaseHook.get_connection("stva")
        print(connection.host)

    test_connection()
    test_virtualenv()
