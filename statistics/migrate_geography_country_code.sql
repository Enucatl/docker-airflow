-- Collapse geography reporting to country_code so MaxMind city rows and
-- country-only Cf-Ipcountry rows do not split the same country.
DROP VIEW IF EXISTS podcast_stats.geography_summary;
CREATE VIEW podcast_stats.geography_summary AS
SELECT country_code,
       count(*) AS requests,
       count(DISTINCT listener_hash) AS listeners,
       coalesce(sum(bytes_sent), 0) AS bytes_sent
FROM podcast_stats.downloads
GROUP BY country_code;
GRANT SELECT ON podcast_stats.geography_summary TO podcast_stats_importer;
