from datetime import UTC, datetime

from automation.pipelines.podcast_statistics import _record


def test_record_maps_loki_event_without_raw_ip() -> None:
    record = _record(
        datetime(2026, 9, 3, tzinfo=UTC),
        {
            "service_name": "barbero-scripts/caddy",
            "geoip_country_code": "US",
            "geoip_country_name": "United States",
        },
        '{"status":200,"size":12,"request":{"method":"GET","uri":"/token-token-token-1234/feed.xml","headers":{"Cf-Connecting-Ip":["8.8.8.8"]}}}',
    )
    assert record is not None
    assert record["request_kind"] == "rss"
    assert record["country_code"] == "US"
    assert record["listener_hash"]
    assert "8.8.8.8" not in str(record)


def test_record_skips_malformed_and_irrelevant_events() -> None:
    assert _record(datetime.now(UTC), {}, "not-json") is None
    assert (
        _record(
            datetime.now(UTC),
            {},
            '{"status":404,"request":{"method":"GET","uri":"/irrelevant"}}',
        )
        is None
    )
