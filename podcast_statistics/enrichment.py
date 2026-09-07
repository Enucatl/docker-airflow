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


def geo_fields(
    metadata: Mapping[str, str],
    *,
    header_country_code: str | None = None,
) -> GeoFields:
    return GeoFields(
        country_code=_text(metadata, "country_code") or (header_country_code or None),
    )
