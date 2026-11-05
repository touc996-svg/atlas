SELECT
    event_id,
    event_type,
    source,
    event_timestamp,
    event_timestamp::date AS event_date,
    EXTRACT(HOUR FROM event_timestamp)::integer AS event_hour,
    payload,
    payload ->> 'sequence' AS sequence_number,
    payload ->> 'message' AS event_message,
    kafka_partition,
    kafka_offset,
    ingested_at
FROM {{ source('atlas', 'events') }}
