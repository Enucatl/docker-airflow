from typing import Any

from podcast_statistics.client import classify_user_agent, listener_hash
from podcast_statistics.enrichment import geo_fields
from podcast_statistics.parsing import parse_media_request


def event(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "ts": 1788464903.7,
        "status": 206,
        "size": 1024,
        "duration": 0.25,
        "request": {
            "method": "GET",
            "uri": "/token-token-token-1234/media/episode-name-0123456789abcdef.mp3",
            "headers": {
                "Cf-Connecting-Ip": ["8.8.8.8"],
                "User-Agent": ["AntennaPod/3.0 (Android)"],
                "Range": ["bytes=10-1033"],
            },
        },
        "resp_headers": {"Content-Length": ["1024"]},
    }
    value.update(overrides)
    return value


def test_parse_media_request_extracts_media_fields() -> None:
    parsed = parse_media_request(event())
    assert parsed is not None
    assert parsed.episode_id == "episode-name"
    assert parsed.range_start == 10
    assert parsed.range_end == 1033
    assert parsed.trusted_client_ip == "8.8.8.8"


def test_parse_media_request_ignores_head_and_non_media() -> None:
    assert parse_media_request(event(status=404)) is None
    assert parse_media_request(event(request={"method": "HEAD"})) is None


def test_client_fields_and_sha256_are_stable() -> None:
    fields = classify_user_agent("AntennaPod/3.0 (Android)")
    assert fields.app_player == "AntennaPod"
    assert fields.operating_system == "Android"
    assert fields.device_category == "Mobile"
    assert listener_hash("8.8.8.8") == listener_hash("8.8.8.8")
    assert listener_hash("8.8.8.8") != listener_hash("8.8.4.4")


def test_geo_fields_are_nullable_and_tolerant() -> None:
    fields = geo_fields({"geoip_country_code": "US", "geoip_latitude": "bad"})
    assert fields.country_code == "US"
    assert fields.latitude is None
    assert geo_fields({}).country_name is None
