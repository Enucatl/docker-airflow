from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from common.loki import query_loki_range_adaptive


@dataclass(frozen=True)
class LokiWindow:
    start: datetime
    end: datetime


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def build_window(
    watermark: datetime,
    now: datetime,
    *,
    ingestion_delay: timedelta = timedelta(minutes=5),
    overlap: timedelta = timedelta(minutes=10),
    max_span: timedelta = timedelta(hours=1),
) -> LokiWindow:
    """Build an eligible, overlapping, bounded Loki query window."""

    if ingestion_delay < timedelta(0) or overlap < timedelta(0):
        raise ValueError("ingestion delay and overlap must not be negative")
    if max_span <= timedelta(0):
        raise ValueError("maximum Loki window span must be positive")

    end = _utc(now) - ingestion_delay
    start = _utc(watermark) - overlap
    if start < end - max_span:
        start = end - max_span
    if start > end:
        start = end
    return LokiWindow(start=start, end=end)


def query_window(
    connection_id: str,
    *,
    query: str,
    window: LokiWindow,
    limit: int = 1000,
) -> list[tuple[datetime, dict[str, str], str]]:
    """Query a bounded window and return sorted stream entries.

    ``query_loki_range_adaptive`` recursively splits full result pages, which
    avoids silently truncating a busy window while retaining a finite request
    size for Loki.
    """

    if limit <= 0:
        raise ValueError("Loki result limit must be positive")
    results = query_loki_range_adaptive(
        connection_id,
        query=query,
        start=_utc(window.start),
        end=_utc(window.end),
        limit=limit,
    )
    entries: list[tuple[datetime, dict[str, str], str]] = []
    for result in results:
        stream = {
            str(key): str(value) for key, value in result.get("stream", {}).items()
        }
        for value in result.get("values", []):
            if not isinstance(value, list) or len(value) < 2:
                continue
            timestamp = datetime.fromtimestamp(int(str(value[0])) / 1_000_000_000, UTC)
            entries.append((timestamp, stream, str(value[1])))
    entries.sort(key=lambda entry: (entry[0], sorted(entry[1].items()), entry[2]))
    return entries
