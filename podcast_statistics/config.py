from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StatisticsConfig:
    """Non-secret settings for the Loki-to-PostgreSQL importer."""

    postgres_connection_id: str = "djangodev"
    loki_connection_id: str = "loki"
    postgres_schema: str = "podcast_stats"


DEFAULT_CONFIG = StatisticsConfig()
