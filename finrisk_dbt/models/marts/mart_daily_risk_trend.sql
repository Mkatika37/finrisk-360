{{ config(materialized='table') }}

select
    processing_date,
    risk_tier,
    count(*) as loan_count,
    avg(risk_score) as avg_risk_score
from {{ ref('stg_loan_risk_scores') }}
group by processing_date, risk_tier
order by processing_date desc
