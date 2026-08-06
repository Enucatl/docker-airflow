from __future__ import annotations

import requests

from automation_core.clients import send_telegram
from automation_core.connections import VaultConnections

INDEX_URL = "https://apt-puppetcore.puppet.com/public/index.html"
PACKAGE_NAME = "puppet8-release-resolute.deb"


def package_is_listed(index_html: str, package_name: str = PACKAGE_NAME) -> bool:
    return package_name in index_html


def render_telegram_message() -> str:
    return f"{PACKAGE_NAME} is available: {INDEX_URL}"


def run(vault: VaultConnections) -> None:
    response = requests.get(INDEX_URL, timeout=30)
    response.raise_for_status()
    if package_is_listed(response.text):
        send_telegram(vault.get("telegram_default"), render_telegram_message())
