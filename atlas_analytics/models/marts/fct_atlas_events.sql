{{-
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        pre_hook=[
            "CREATE INDEX IF NOT EXISTS idx_atlas_events_ingested_at ON {{ source('atlas', 'events') }} (ingested_at)"
        ],
        post_hook=[
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_atlas_events_event_id ON {{ this }} (event_id)"
        ]
    )
-}}

{% set lookback_hours = var('atlas_incremental_lookback_hours', 2) | int %}
{% if lookback_hours < 1 %}
    {{ exceptions.raise_compiler_error(
        "atlas_incremental_lookback_hours must be a positive integer (>= 1)."
    ) }}
{% endif %}

WITH source_events AS (
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

    {% if is_incremental() %}
    WHERE ingested_at >= (
        SELECT
            COALESCE(
                MAX(ingested_at),
                '1900-01-01'::timestamptz
            ) - make_interval(hours => {{ lookback_hours }})
        FROM {{ this }}
    )
    {% endif %}
)

SELECT *
FROM source_events
