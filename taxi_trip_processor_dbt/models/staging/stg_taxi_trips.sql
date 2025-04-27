{{ config(materialized='view') }}

SELECT
  CAST(id AS INTEGER) AS id,
  trip_id,
  taxi_id,
  trip_start_timestamp,
  trip_end_timestamp,
  trip_miles,
  trip_total
FROM {{ source('raw', 'taxi_rides') }}
WHERE trip_total > 0
