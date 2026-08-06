from __future__ import annotations

from email.message import EmailMessage
import html
import smtplib
from typing import TYPE_CHECKING, Any

import requests

from automation_core.connections import Connection, VaultConnections

if TYPE_CHECKING:
    import psycopg


def postgres_connect(connection: Connection) -> psycopg.Connection[Any]:
    import psycopg

    return psycopg.connect(
        host=connection.host,
        port=connection.port or 5432,
        user=connection.login,
        password=connection.password,
        dbname=connection.extra.get("dbname") or connection.extra.get("database"),
    )


def send_telegram(
    connection: Connection, text: str, *, parse_mode: str | None = None
) -> None:
    payload: dict[str, str] = {"chat_id": connection.host, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    response = requests.post(
        f"https://api.telegram.org/bot{connection.password}/sendMessage",
        data=payload,
        timeout=30,
    )
    response.raise_for_status()


def sanitize_failure(pipeline: str, error: BaseException) -> str:
    error_type = type(error).__name__
    return (
        f"<b>{html.escape(pipeline)} failed</b>\n<pre>{html.escape(error_type)}</pre>"
    )


def notify_failure(
    vault: VaultConnections, pipeline: str, error: BaseException
) -> None:
    send_telegram(
        vault.get("telegram_default"),
        sanitize_failure(pipeline, error),
        parse_mode="HTML",
    )


def send_email(
    connection: Connection, *, sender: str, recipient: str, subject: str, body: str
) -> None:
    message = EmailMessage()
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = subject
    message.set_content(body)
    message.add_alternative(f"<pre>{html.escape(body)}</pre>", subtype="html")
    with smtplib.SMTP(connection.host, connection.port or 587, timeout=60) as smtp:
        smtp.starttls()
        if connection.login:
            smtp.login(connection.login, connection.password)
        smtp.send_message(message)
