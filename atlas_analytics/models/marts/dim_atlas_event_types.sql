SELECT
    event_type,
    source,
    COUNT(*) AS total_events,
    MIN(event_timestamp) AS first_event_at,
    MAX(event_timestamp) AS last_event_at,
    COUNT(DISTINCT event_date) AS active_days
FROM {{ ref('stg_atlas_events') }}
GROUP BY event_type, source
