from datetime import UTC, datetime, timedelta
from typing import Any

from podcast_statistics.loki import LokiWindow, build_window, query_window


def test_build_window_applies_delay_overlap_and_maximum_span() -> None:
    now = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    window = build_window(datetime(2026, 9, 3, 11, 55, tzinfo=UTC), now)

    assert window == LokiWindow(
        start=datetime(2026, 9, 3, 11, 45, tzinfo=UTC),
        end=datetime(2026, 9, 3, 11, 55, tzinfo=UTC),
    )


def test_build_window_caps_first_run() -> None:
    now = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)

    assert build_window(datetime(1970, 1, 1, tzinfo=UTC), now).start == (
        now - timedelta(hours=1, minutes=5)
    )


def test_query_window_flattens_and_sorts_adaptive_results(monkeypatch) -> None:
    def fake_query(connection_id: str, **kwargs: object) -> list[dict[str, Any]]:
        assert connection_id == "loki"
        return [
            {"stream": {"service_name": "b"}, "values": [["2000000000", "later"]]},
            {"stream": {"service_name": "a"}, "values": [["1000000000", "earlier"]]},
        ]

    monkeypatch.setattr("podcast_statistics.loki.query_loki_range_adaptive", fake_query)
    entries = query_window(
        "loki",
        query='{service_name="barbero-scripts/caddy"}',
        window=LokiWindow(
            datetime(2026, 9, 3, 11, 0, tzinfo=UTC),
            datetime(2026, 9, 3, 11, 1, tzinfo=UTC),
        ),
    )

    assert [entry[2] for entry in entries] == ["earlier", "later"]
