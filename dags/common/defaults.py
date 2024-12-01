import os
from datetime import timedelta

default_args = {
    "depends_on_past": False,
    "email": [os.environ.get("AIRFLOW__SMTP__SMTP_USER")],  # Replace with your email
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "venv_cache_path": "/opt/airflow/venv",
}
