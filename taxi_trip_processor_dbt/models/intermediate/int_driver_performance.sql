-- models/intermediate/int_driver_shifts.sql
{{ config(
    materialized='incremental',
    unique_key=['taxi_id', 'trip_id'],
    pre_hook=[
        "{% if is_incremental() and not flags.FULL_REFRESH %}DROP TABLE IF EXISTS {{ this }};{% endif %}"
    ]
) }}

WITH trip_with_previous AS (
    SELECT
        taxi_id,
        trip_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_miles,
        trip_total,
        LEAD(trip_start_timestamp) OVER (PARTITION BY taxi_id ORDER BY trip_start_timestamp) AS next_trip_start,
        LAG(trip_end_timestamp) OVER (PARTITION BY taxi_id ORDER BY trip_start_timestamp) AS previous_trip_end
    FROM {{ ref('stg_taxi_trips') }}
    {% if is_incremental() %}
      WHERE trip_start_timestamp > (SELECT MAX(trip_start_timestamp) FROM {{ this }})
    {% endif %}
),
shift_start_end AS (
    SELECT
        taxi_id,
        trip_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_miles,
        trip_total,
        CASE
            WHEN previous_trip_end IS NULL OR EXTRACT(EPOCH FROM (trip_start_timestamp - previous_trip_end)) >= 3600 THEN 1  -- 3600 seconds = 60 minutes
            ELSE 0
        END AS new_shift  -- Identifies when a new shift begins (based on 60-minute gap)
    FROM trip_with_previous
),
shift_with_row_number AS (
    SELECT
        taxi_id,
        trip_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_miles,
        trip_total,
        CASE
            WHEN previous_trip_end IS NULL OR EXTRACT(EPOCH FROM (trip_start_timestamp - previous_trip_end)) >= 3600 THEN 1
            ELSE 0
        END AS new_shift,
        SUM(
            CASE
                WHEN previous_trip_end IS NULL OR EXTRACT(EPOCH FROM (trip_start_timestamp - previous_trip_end)) >= 3600 THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY taxi_id ORDER BY trip_start_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS shift_number
    FROM trip_with_previous
)
SELECT
    taxi_id,
    trip_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_miles,
    trip_total,
    shift_number
FROM shift_with_row_number