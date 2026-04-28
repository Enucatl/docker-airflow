from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task

from common.telegram import telegram_message_task


def test_telegram_message_task_uses_default_connection() -> None:
    operator = telegram_message_task(task_id="send_report", text="Hello")

    assert operator.task_id == "send_report"
    assert operator.telegram_conn_id == "telegram_default"
    assert operator.text == "Hello"


def test_telegram_message_task_composes_with_taskflow_output() -> None:
    with DAG("telegram_helper_test", start_date=datetime(2026, 1, 1)) as dag:

        @task(task_id="render_report")
        def render_report() -> str:
            return "Hello from TaskFlow"

        send_report = telegram_message_task(
            task_id="send_report",
            text=render_report(),
        )

    assert send_report.upstream_task_ids == {"render_report"}
    assert set(dag.task_ids) == {"render_report", "send_report"}
