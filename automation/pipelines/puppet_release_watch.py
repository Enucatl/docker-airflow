from __future__ import annotations

import requests

from automation_core.clients import send_telegram
from automation_core.connections import VaultConnections

INDEX_URL = "https://apt-puppetcore.puppet.com/public/index.html"
PACKAGE_NAMES = ("puppet8-release-resolute.deb", "puppet9-release-resolute.deb")


def package_is_listed(index_html: str, package_name: str) -> bool:
    return package_name in index_html


def render_telegram_message(package_name: str) -> str:
    return f"{package_name} is available: {INDEX_URL}"


def run(vault: VaultConnections) -> None:
    response = requests.get(INDEX_URL, timeout=30)
    response.raise_for_status()
    package_name = next(
        (name for name in PACKAGE_NAMES if package_is_listed(response.text, name)),
        None,
    )
    if package_name:
        send_telegram(
            vault.get("telegram_default"), render_telegram_message(package_name)
        )
