-- Barbero podcast statistics storage.
-- Apply as a database administrator to the djangodev database.

CREATE SCHEMA IF NOT EXISTS podcast_stats;

CREATE TABLE IF NOT EXISTS podcast_stats.downloads (
    id                    bigserial PRIMARY KEY,
    source_log_id         text NOT NULL UNIQUE,
    observed_at           timestamptz NOT NULL,
    request_kind          text NOT NULL DEFAULT 'media',
    episode_id            text,
    method                text NOT NULL,
    status_code           integer NOT NULL,
    request_path          text NOT NULL,
    bytes_sent            bigint NOT NULL DEFAULT 0,
    content_length        bigint,
    range_start           bigint,
    range_end             bigint,
    request_duration_ms   double precision,
    cloudflare             jsonb,
    user_agent             text,
    app_player             text,
    browser               text,
    operating_system      text,
    device_category       text,
    listener_hash         text,
    country_code          text,
    country_name          text,
    city                  text,
    continent              text,
    subdivision            text,
    timezone               text,
    postal_code            text,
    latitude              double precision,
    longitude             double precision,
    CHECK (request_kind IN ('media', 'page', 'rss')),
    CHECK (bytes_sent >= 0),
    CHECK (content_length IS NULL OR content_length >= 0),
    CHECK (range_start IS NULL OR range_start >= 0),
    CHECK (range_end IS NULL OR range_end >= range_start)
);

CREATE INDEX IF NOT EXISTS downloads_episode_time_idx
    ON podcast_stats.downloads (episode_id, observed_at);

CREATE INDEX IF NOT EXISTS downloads_listener_time_idx
    ON podcast_stats.downloads (listener_hash, observed_at);

CREATE INDEX IF NOT EXISTS downloads_country_time_idx
    ON podcast_stats.downloads (country_code, observed_at);

CREATE INDEX IF NOT EXISTS downloads_kind_time_idx
    ON podcast_stats.downloads (request_kind, observed_at);

CREATE TABLE IF NOT EXISTS podcast_stats.importer_state (
    state_key       text PRIMARY KEY,
    watermark       timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (state_key = 'loki_caddy')
);

INSERT INTO podcast_stats.importer_state (state_key, watermark)
VALUES ('loki_caddy', 'epoch')
ON CONFLICT (state_key) DO NOTHING;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'podcast_stats_importer'
    ) THEN
        CREATE ROLE podcast_stats_importer NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA podcast_stats TO podcast_stats_importer;
GRANT SELECT, INSERT, UPDATE ON podcast_stats.downloads TO podcast_stats_importer;
GRANT SELECT, UPDATE ON podcast_stats.importer_state TO podcast_stats_importer;
GRANT USAGE, SELECT ON SEQUENCE podcast_stats.downloads_id_seq
    TO podcast_stats_importer;

ALTER DEFAULT PRIVILEGES IN SCHEMA podcast_stats
    GRANT SELECT, INSERT, UPDATE ON TABLES TO podcast_stats_importer;
ALTER DEFAULT PRIVILEGES IN SCHEMA podcast_stats
    GRANT USAGE, SELECT ON SEQUENCES TO podcast_stats_importer;
