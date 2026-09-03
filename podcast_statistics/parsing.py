from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any
from urllib.parse import urlsplit


_MEDIA_PATH = re.compile(
    r"^/[A-Za-z0-9_-]{16,128}/media/(?P<episode>.+)-[0-9a-f]{16}\.mp3$"
)
_RANGE = re.compile(r"^bytes=(?P<start>\d+)-(?P<end>\d*)$")


@dataclass(frozen=True)
class MediaRequest:
    observed_at: str
    episode_id: str
    method: str
    status_code: int
    request_path: str
    bytes_sent: int
    content_length: int | None
    range_start: int | None
    range_end: int | None
    request_duration_ms: float | None
    user_agent: str | None
    trusted_client_ip: str | None
    cloudflare: dict[str, str]


def _header(headers: dict[str, Any], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() != name.lower():
            continue
        if isinstance(value, list):
            return str(value[0]) if value else None
        return str(value)
    return None


def _integer(value: Any, field: str, *, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid Caddy {field}") from error
    if parsed < 0:
        raise ValueError(f"negative Caddy {field}")
    return parsed


def parse_media_request(event: dict[str, Any]) -> MediaRequest | None:
    request = event.get("request")
    if not isinstance(request, dict):
        raise ValueError("Caddy event has no request object")
    method = str(request.get("method") or "")
    if method == "HEAD":
        return None
    if method != "GET" or int(event.get("status", 0)) not in (200, 206):
        return None
    path = urlsplit(str(request.get("uri") or "")).path
    match = _MEDIA_PATH.fullmatch(path)
    if match is None:
        return None

    request_headers = request.get("headers") or {}
    response_headers = event.get("resp_headers") or {}
    range_value = _header(request_headers, "Range")
    range_start: int | None = None
    range_end: int | None = None
    if range_value:
        range_match = _RANGE.fullmatch(range_value)
        if range_match is None:
            raise ValueError("invalid HTTP range")
        range_start = int(range_match.group("start"))
        end = range_match.group("end")
        range_end = int(end) if end else None

    cloudflare = {
        key.lower(): value
        for key, value in (
            ("cf-ray", _header(request_headers, "Cf-Ray")),
            ("cf-cache-status", _header(request_headers, "Cf-Cache-Status")),
            ("cf-country", _header(request_headers, "Cf-Country")),
        )
        if value is not None
    }
    duration = event.get("duration")
    return MediaRequest(
        observed_at=str(event.get("ts") or ""),
        episode_id=match.group("episode"),
        method=method,
        status_code=int(event["status"]),
        request_path=path,
        bytes_sent=_integer(event.get("size"), "size"),
        content_length=_integer(
            _header(response_headers, "Content-Length"), "content length", default=0
        )
        or None,
        range_start=range_start,
        range_end=range_end,
        request_duration_ms=float(duration) * 1000 if duration is not None else None,
        user_agent=_header(request_headers, "User-Agent"),
        trusted_client_ip=_header(request_headers, "Cf-Connecting-Ip"),
        cloudflare=cloudflare,
    )
