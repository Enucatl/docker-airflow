-- Extend an already-created statistics database for page and RSS requests.
ALTER TABLE podcast_stats.downloads
    ADD COLUMN IF NOT EXISTS request_kind text NOT NULL DEFAULT 'media';
ALTER TABLE podcast_stats.downloads
    ALTER COLUMN episode_id DROP NOT NULL;
ALTER TABLE podcast_stats.downloads
    ADD CONSTRAINT downloads_request_kind_check
    CHECK (request_kind IN ('media', 'page', 'rss'));
CREATE INDEX IF NOT EXISTS downloads_kind_time_idx
    ON podcast_stats.downloads (request_kind, observed_at);
