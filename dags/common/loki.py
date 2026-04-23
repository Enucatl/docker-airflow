from __future__ import annotations

from datetime import datetime, timedelta
import logging
from typing import Any

import requests
from airflow.sdk.bases.hook import BaseHook

from common.ssl import verify


def query_loki_range(
    conn_id: str,
    *,
    query: str,
    start: datetime,
    end: datetime,
    limit: int = 1000,
) -> dict[str, Any]:
    conn = BaseHook.get_connection(conn_id)
    endpoint = f"{conn.host.rstrip('/')}/loki/api/v1/query_range"
    headers: dict[str, str] = {}
    extras = conn.extra_dejson
    if tenant_id := extras.get("tenant_id"):
        headers["X-Scope-OrgID"] = tenant_id
    auth = (conn.login, conn.password) if conn.login and conn.password else None
    response = requests.get(
        endpoint,
        params={
            "query": query,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": limit,
            "direction": "forward",
        },
        headers=headers,
        auth=auth,
        verify=verify,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def _count_loki_entries(results: list[dict[str, Any]]) -> int:
    return sum(len(result.get("values", [])) for result in results)


def query_loki_range_adaptive(
    conn_id: str,
    *,
    query: str,
    start: datetime,
    end: datetime,
    limit: int = 1000,
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    def fetch_window(
        window_start: datetime, window_end: datetime
    ) -> list[dict[str, Any]]:
        payload = query_loki_range(
            conn_id,
            query=query,
            start=window_start,
            end=window_end,
            limit=limit,
        )
        results = payload.get("data", {}).get("result", [])
        entry_count = _count_loki_entries(results)
        if logger:
            logger.info(
                "Loki window %s through %s returned %s streams and %s entries",
                window_start.isoformat(),
                window_end.isoformat(),
                len(results),
                entry_count,
            )
        if entry_count < limit:
            return results
        window_span = window_end - window_start
        if window_span <= timedelta(microseconds=1):
            if logger:
                logger.warning(
                    "Loki window %s through %s reached the entry limit; returning "
                    "possibly truncated results",
                    window_start.isoformat(),
                    window_end.isoformat(),
                )
            return results

        midpoint = window_start + (window_span / 2)
        next_start = midpoint + timedelta(microseconds=1)
        if next_start >= window_end:
            if logger:
                logger.warning(
                    "Loki window %s through %s cannot be split further; returning "
                    "possibly truncated results",
                    window_start.isoformat(),
                    window_end.isoformat(),
                )
            return results

        if logger:
            logger.info(
                "Loki window %s through %s hit the %s entry limit; splitting into "
                "%s through %s and %s through %s",
                window_start.isoformat(),
                window_end.isoformat(),
                limit,
                window_start.isoformat(),
                midpoint.isoformat(),
                next_start.isoformat(),
                window_end.isoformat(),
            )
        left_results = fetch_window(window_start, midpoint)
        right_results = fetch_window(next_start, window_end)
        return left_results + right_results

    return fetch_window(start, end)
