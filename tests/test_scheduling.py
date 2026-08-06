from datetime import UTC, datetime
from pathlib import Path

from automation.pipelines.download_zanzara import scheduled_date_range


def test_zanzara_scheduled_range_includes_previous_day_and_boundary() -> None:
    assert scheduled_date_range(datetime(2026, 8, 6, 12, tzinfo=UTC)) == (
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )


def test_timers_are_utc_and_do_not_catch_up() -> None:
    yaml = Path("../puppet-control-repo/data/nodes/docker.yaml").read_text()
    assert yaml.count("Persistent=false") >= 4
    assert "OnCalendar=*-*-* *:00:00 UTC" in yaml
    assert "OnCalendar=*-*-01 03:00:00 UTC" in yaml
    assert yaml.count("docker compose run --build --rm --no-deps") >= 4
