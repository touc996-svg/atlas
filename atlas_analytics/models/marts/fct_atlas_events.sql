SELECT
    event_id,
    event_type,
    source,
    event_timestamp,
    event_date,
    event_hour,
    sequence_number,
    event_message,
    payload,
    kafka_partition,
    kafka_offset,
    ingested_at
FROM {{ ref('stg_atlas_events') }}
