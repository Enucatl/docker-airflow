"""Opt-in integration checks for the deployed statistics services."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
from typing import Any

import psycopg
import pytest
import requests


@pytest.mark.skipif(
    not os.getenv("STATISTICS_TEST_LOKI_URL"),
    reason="set STATISTICS_TEST_LOKI_URL to run Loki integration checks",
)
def test_loki_returns_parseable_caddy_payload() -> None:
    end = datetime.now(UTC)
    response = requests.get(
        f"{os.environ['STATISTICS_TEST_LOKI_URL'].rstrip('/')}/loki/api/v1/query_range",
        params={
            "query": '{service_name="barbero-scripts/caddy"}',
            "start": (end - timedelta(minutes=30)).isoformat(),
            "end": end.isoformat(),
            "limit": 10,
            "direction": "backward",
        },
        # The local deployment uses a private CA; this test is explicitly opt-in.
        verify=False,
        timeout=30,
    )
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    assert payload["status"] == "success"
    for result in payload["data"]["result"]:
        assert "service_name" in result["stream"]
        assert all(len(value) >= 2 for value in result["values"])


@pytest.mark.skipif(
    not os.getenv("STATISTICS_TEST_DSN"),
    reason="set STATISTICS_TEST_DSN to run PostgreSQL integration checks",
)
def test_postgres_statistics_schema_and_reporting_views() -> None:
    with psycopg.connect(os.environ["STATISTICS_TEST_DSN"]) as database:
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'podcast_stats' AND table_name = 'downloads'"
            )
            columns = {row[0] for row in cursor.fetchall()}
            assert {"source_log_id", "request_kind", "listener_hash"} <= columns
            cursor.execute(
                "SELECT table_name FROM information_schema.views "
                "WHERE table_schema = 'podcast_stats'"
            )
            views = {row[0] for row in cursor.fetchall()}
            assert {
                "daily_summary",
                "episode_summary",
                "geography_summary",
                "client_summary",
                "episode_completion_estimate",
            } <= views
