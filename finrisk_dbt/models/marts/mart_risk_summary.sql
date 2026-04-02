{{ config(materialized='table') }}

select
    risk_tier,
    count(*) as loan_count,
    avg(risk_score) as avg_risk_score,
    avg(loan_amount) as avg_loan_amount,
    avg(ltv) as avg_ltv,
    avg(dti) as avg_dti,
    avg(interest_rate) as avg_interest_rate
from {{ ref('stg_loan_risk_scores') }}
group by risk_tier
order by avg_risk_score desc
