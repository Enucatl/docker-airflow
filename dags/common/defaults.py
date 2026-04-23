from datetime import timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier

SMTP_CONN_ID = "smtp_default"
ALERT_FROM = "gmatteo.abis+airflow.docker.home.arpa@gmail.com"
ALERT_TO = "m.app.logins@pm.me"

default_args: dict[str, object] = {
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
    "venv_cache_path": "/opt/airflow/venv",
}

default_args["on_failure_callback"] = SmtpNotifier(
    to=ALERT_TO,
    from_email=ALERT_FROM,
    smtp_conn_id=SMTP_CONN_ID,
)
