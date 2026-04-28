from __future__ import annotations

from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.sdk import XComArg

TELEGRAM_CONN_ID = "telegram_default"


def telegram_message_task(
    *,
    task_id: str,
    text: str | XComArg,
    telegram_conn_id: str = TELEGRAM_CONN_ID,
) -> TelegramOperator:
    return TelegramOperator(
        task_id=task_id,
        telegram_conn_id=telegram_conn_id,
        text=text,
    )
