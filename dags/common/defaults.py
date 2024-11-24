import os

default_args = {
    "depends_on_past": False,
    "email": [os.environ.get("AIRFLOW__SMTP__SMTP_USER")],  # Replace with your email
    "retries": 0,
    "venv_cache_path": "/opt/airflow/venv",
}
