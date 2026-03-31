-- marts/fct_risk_indicators.sql
-- Mart model: aggregate risk indicators for downstream consumption

{{ config(materialized='table') }}

SELECT
    series_id,
    DATE_TRUNC('month', observation_date)  AS month,
    AVG(value)                             AS avg_value,
    MIN(value)                             AS min_value,
    MAX(value)                             AS max_value,
    COUNT(*)                               AS observation_count
FROM {{ ref('stg_fred_series') }}
GROUP BY 1, 2
