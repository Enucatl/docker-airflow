from podcast_statistics.config import DEFAULT_CONFIG, StatisticsConfig


def test_statistics_config_uses_existing_vault_connections() -> None:
    assert DEFAULT_CONFIG == StatisticsConfig(
        postgres_connection_id="djangodev",
        loki_connection_id="loki",
        postgres_schema="podcast_stats",
    )
