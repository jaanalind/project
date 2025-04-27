{{ config(materialized='incremental', unique_key=['taxi_id', 'shift_start']) }}

SELECT
    taxi_id,
    MIN(trip_start_timestamp) AS shift_start,
    MAX(trip_end_timestamp) AS shift_end,
    COUNT(*) AS trip_count,
    SUM(trip_miles) AS total_distance,
    SUM(trip_total) AS total_fare,
    EXTRACT(EPOCH FROM (MAX(trip_end_timestamp) - MIN(trip_start_timestamp))) / 3600 AS shift_duration_hours
FROM {{ ref('int_driver_performance') }}
GROUP BY taxi_id, shift_number
ORDER BY shift_start DESC 