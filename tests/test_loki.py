from __future__ import annotations

from datetime import UTC, datetime

from common.loki import query_loki_range, query_loki_range_adaptive


def test_query_loki_range_uses_shared_ca_bundle(monkeypatch) -> None:
    class DummyConnection:
        host = "https://loki.example"
        login = "user"
        password = "pass"
        extra_dejson = {"tenant_id": "tenant-1"}

    captured: dict[str, object] = {}

    def fake_get_connection(conn_id: str) -> DummyConnection:
        assert conn_id == "loki"
        return DummyConnection()

    def fake_get(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {"ok": True}

        return Response()

    monkeypatch.setattr("common.loki.BaseHook.get_connection", fake_get_connection)
    monkeypatch.setattr("common.loki.requests.get", fake_get)

    result = query_loki_range(
        "loki",
        query='{job="suricata"}',
        start=datetime(2026, 3, 1, tzinfo=UTC),
        end=datetime(2026, 4, 1, tzinfo=UTC),
        limit=1,
    )

    assert result == {"ok": True}
    assert captured["kwargs"]["verify"] == "/etc/ssl/certs/ca-certificates.crt"


def test_query_loki_range_adaptive_splits_when_limit_hit(monkeypatch) -> None:
    calls: list[tuple[datetime, datetime, int]] = []

    def fake_query_loki_range(
        conn_id: str,
        *,
        query: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> dict[str, object]:
        assert conn_id == "loki"
        assert query == '{job="suricata"}'
        calls.append((start, end, limit))
        if len(calls) == 1:
            return {
                "data": {
                    "result": [
                        {
                            "stream": {"chunk": "root"},
                            "values": [
                                [start.isoformat(), "line"] for _ in range(limit)
                            ],
                        }
                    ]
                }
            }
        return {
            "data": {
                "result": [
                    {
                        "stream": {"chunk": "child"},
                        "values": [
                            [start.isoformat(), "line"],
                            [end.isoformat(), "line"],
                        ],
                    }
                ]
            }
        }

    monkeypatch.setattr("common.loki.query_loki_range", fake_query_loki_range)

    results = query_loki_range_adaptive(
        "loki",
        query='{job="suricata"}',
        start=datetime(2026, 3, 1, tzinfo=UTC),
        end=datetime(2026, 4, 1, tzinfo=UTC),
        limit=5000,
    )

    assert len(calls) == 3
    assert sum(len(result["values"]) for result in results) == 4
