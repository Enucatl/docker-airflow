from __future__ import annotations

from datetime import datetime
import hashlib
import json
from typing import Any, Iterable, Mapping, Protocol


class Database(Protocol):
    def transaction(self) -> Any: ...

    def cursor(self) -> Any: ...


DOWNLOAD_SQL = """
INSERT INTO podcast_stats.downloads (
    source_log_id, observed_at, request_kind, episode_id, method, status_code,
    request_path, bytes_sent, content_length, range_start, range_end,
    request_duration_ms, cloudflare, user_agent, app_player, browser,
    operating_system, device_category, listener_hash, country_code,
    country_name, city, continent, subdivision, timezone, postal_code,
    latitude, longitude
) VALUES (
    %(source_log_id)s, %(observed_at)s, %(request_kind)s, %(episode_id)s,
    %(method)s, %(status_code)s, %(request_path)s, %(bytes_sent)s,
    %(content_length)s, %(range_start)s, %(range_end)s,
    %(request_duration_ms)s, %(cloudflare)s, %(user_agent)s, %(app_player)s,
    %(browser)s, %(operating_system)s, %(device_category)s,
    %(listener_hash)s, %(country_code)s, %(country_name)s, %(city)s,
    %(continent)s, %(subdivision)s, %(timezone)s, %(postal_code)s,
    %(latitude)s, %(longitude)s
)
ON CONFLICT (source_log_id) DO NOTHING
"""

STATE_SQL = """
UPDATE podcast_stats.importer_state
SET watermark = GREATEST(watermark, %(watermark)s), updated_at = now()
WHERE state_key = 'loki_caddy'
"""


def source_log_id(timestamp: str, stream: Mapping[str, str], line: str) -> str:
    payload = json.dumps(
        {"line": line, "stream": dict(sorted(stream.items())), "timestamp": timestamp},
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def insert_records(
    database: Database,
    records: Iterable[Mapping[str, Any]],
    *,
    watermark: datetime,
) -> None:
    rows = list(records)
    with database.transaction():
        with database.cursor() as cursor:
            if rows:
                cursor.executemany(DOWNLOAD_SQL, rows)
            cursor.execute(STATE_SQL, {"watermark": watermark})
