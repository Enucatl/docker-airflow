from __future__ import annotations

from dataclasses import dataclass
import hashlib


@dataclass(frozen=True)
class ClientFields:
    app_player: str
    browser: str
    operating_system: str
    device_category: str


def classify_user_agent(user_agent: str | None) -> ClientFields:
    value = user_agent or ""
    lower = value.lower()
    if "antennapod" in lower:
        app = "AntennaPod"
    elif "overcast" in lower:
        app = "Overcast"
    elif "apple podcasts" in lower or "podcasts/" in lower:
        app = "Apple Podcasts"
    elif "mozilla" in lower:
        app = "Web browser"
    else:
        app = "Unknown"

    browser = "Unknown"
    if "chrome/" in lower or "crios/" in lower:
        browser = "Chrome"
    elif "firefox/" in lower or "fxios/" in lower:
        browser = "Firefox"
    elif "safari/" in lower and "chrome/" not in lower:
        browser = "Safari"

    operating_system = "Unknown"
    if "windows" in lower:
        operating_system = "Windows"
    elif "android" in lower:
        operating_system = "Android"
    elif "iphone" in lower or "ipad" in lower or "ios" in lower:
        operating_system = "iOS"
    elif "mac os" in lower or "macintosh" in lower:
        operating_system = "macOS"
    elif "linux" in lower:
        operating_system = "Linux"

    device = (
        "Mobile"
        if "mobile" in lower or "iphone" in lower or "android" in lower
        else "Desktop"
    )
    if not value:
        device = "Unknown"
    return ClientFields(app, browser, operating_system, device)


def listener_hash(client_ip: str | None) -> str | None:
    if not client_ip:
        return None
    return hashlib.sha256(client_ip.encode("utf-8")).hexdigest()
