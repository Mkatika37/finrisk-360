{{ config(materialized='table') }}

select *
from {{ ref('stg_loan_risk_scores') }}
where risk_tier in ('CRITICAL', 'HIGH')
order by risk_score desc
