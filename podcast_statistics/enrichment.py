from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class GeoFields:
    country_code: str | None = None
    country_name: str | None = None
    city: str | None = None
    continent: str | None = None
    subdivision: str | None = None
    timezone: str | None = None
    postal_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None


def _text(metadata: Mapping[str, str], key: str) -> str | None:
    value = metadata.get(key)
    return value or None


def _float(metadata: Mapping[str, str], key: str) -> float | None:
    value = _text(metadata, key)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def geo_fields(metadata: Mapping[str, str]) -> GeoFields:
    return GeoFields(
        country_code=_text(metadata, "geoip_country_code"),
        country_name=_text(metadata, "geoip_country_name"),
        city=_text(metadata, "geoip_city"),
        continent=_text(metadata, "geoip_continent"),
        subdivision=_text(metadata, "geoip_subdivision"),
        timezone=_text(metadata, "geoip_timezone"),
        postal_code=_text(metadata, "geoip_postal_code"),
        latitude=_float(metadata, "geoip_latitude"),
        longitude=_float(metadata, "geoip_longitude"),
    )
