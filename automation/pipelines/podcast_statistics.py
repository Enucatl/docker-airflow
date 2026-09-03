from __future__ import annotations

import json
import logging

from automation_core.clients import postgres_connect
from automation_core.connections import VaultConnections
from common.loki import set_vault
from podcast_statistics.client import classify_user_agent, listener_hash
from podcast_statistics.config import DEFAULT_CONFIG
from podcast_statistics.enrichment import geo_fields
from podcast_statistics.loki import build_window, query_window
from podcast_statistics.parsing import parse_request
from podcast_statistics.storage import insert_records, source_log_id

logger = logging.getLogger(__name__)


def _watermark(database) -> object:
    with database.cursor() as cursor:
        cursor.execute(
            "SELECT watermark FROM podcast_stats.importer_state "
            "WHERE state_key = 'loki_caddy'"
        )
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError("podcast statistics watermark is missing")
    return row[0]


def _record(timestamp, stream: dict[str, str], line: str) -> dict[str, object] | None:
    try:
        event = json.loads(line)
        parsed = parse_request(event)
    except TypeError, ValueError, json.JSONDecodeError:
        logger.warning("statistics event parse failure")
        return None
    if parsed is None:
        return None
    client = classify_user_agent(parsed.user_agent)
    geo = geo_fields(stream)
    return {
        "source_log_id": source_log_id(str(timestamp.timestamp()), stream, line),
        "observed_at": timestamp,
        "request_kind": parsed.request_kind,
        "episode_id": parsed.episode_id,
        "method": parsed.method,
        "status_code": parsed.status_code,
        "request_path": parsed.request_path,
        "bytes_sent": parsed.bytes_sent,
        "content_length": parsed.content_length,
        "range_start": parsed.range_start,
        "range_end": parsed.range_end,
        "request_duration_ms": parsed.request_duration_ms,
        "cloudflare": parsed.cloudflare,
        "user_agent": parsed.user_agent,
        "app_player": client.app_player,
        "browser": client.browser,
        "operating_system": client.operating_system,
        "device_category": client.device_category,
        "listener_hash": listener_hash(parsed.trusted_client_ip),
        "country_code": geo.country_code,
        "country_name": geo.country_name,
        "city": geo.city,
        "continent": geo.continent,
        "subdivision": geo.subdivision,
        "timezone": geo.timezone,
        "postal_code": geo.postal_code,
        "latitude": geo.latitude,
        "longitude": geo.longitude,
    }


def run(vault: VaultConnections) -> None:
    """Import eligible Caddy requests from Loki into PostgreSQL."""

    set_vault(vault)
    with postgres_connect(vault.get(DEFAULT_CONFIG.postgres_connection_id)) as database:
        watermark = _watermark(database)
        from datetime import UTC, datetime

        now = datetime.now(UTC)
        window = build_window(watermark, now)
        entries = query_window(
            DEFAULT_CONFIG.loki_connection_id,
            query='{service_name="barbero-scripts/caddy"}',
            window=window,
        )
        records = [
            record
            for timestamp, stream, line in entries
            if (record := _record(timestamp, stream, line)) is not None
        ]
        event_watermark = max((entry[0] for entry in entries), default=watermark)
        insert_records(database, records, watermark=event_watermark)
        logger.info(
            "imported %d statistics records from %s through %s",
            len(records),
            window.start.isoformat(),
            window.end.isoformat(),
        )
