SELECT entity_id, observed_at, source_revision, metric_value, status, reading
FROM local_catalog.analytics.quickstart_observation_upsert
ORDER BY entity_id, observed_at
