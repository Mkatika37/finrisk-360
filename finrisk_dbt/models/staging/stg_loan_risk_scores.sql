{{ config(materialized='view') }}

select
    loan_amount,
    ltv,
    dti,
    interest_rate,
    income,
    ltv_score,
    dti_score,
    rate_spread_score,
    macro_stress_score,
    risk_score,
    risk_tier,
    processing_date,
    action_taken as action_code,
    loan_type as loan_type_code,
    CURRENT_TIMESTAMP() as processed_at
from {{ source('finrisk360', 'loan_risk_scores') }}
