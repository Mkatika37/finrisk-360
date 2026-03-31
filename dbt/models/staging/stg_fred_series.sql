-- staging/stg_fred_series.sql
-- Staging model: clean and deduplicate raw FRED data

{{ config(materialized='view') }}

SELECT
    series_id,
    observation_date,
    CAST(value AS FLOAT)       AS value,
    CURRENT_TIMESTAMP()        AS _loaded_at
FROM {{ source('raw', 'fred_series') }}
WHERE value IS NOT NULL
