from __future__ import annotations

from datetime import datetime

import requests
from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowSkipException

from common.defaults import default_args
from common.telegram import telegram_message_task


INDEX_URL = "https://apt-puppetcore.puppet.com/public/index.html"
PACKAGE_NAME = "puppet8-release-resolute.deb"


def render_telegram_message() -> str:
    return f"{PACKAGE_NAME} is available: {INDEX_URL}"


def package_is_listed(index_html: str, package_name: str = PACKAGE_NAME) -> bool:
    return package_name in index_html


with DAG(
    "puppet_release_watch",
    default_args=default_args,
    description="Checks for the Puppet 8 Resolute release package and alerts on Telegram",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
) as dag:

    @task(task_id="check_release")
    def check_release() -> str:
        response = requests.get(INDEX_URL, timeout=30)
        response.raise_for_status()

        if not package_is_listed(response.text):
            raise AirflowSkipException(f"{PACKAGE_NAME} is not listed yet.")

        return render_telegram_message()

    send_telegram = telegram_message_task(
        task_id="send_telegram",
        text=check_release(),
    )
