import pytest

def calculate_risk_score(ltv, dti, interest_rate, macro=0.6):
    ltv_score = 0.2 if ltv < 80 else 0.5 if ltv < 90 \
                else 0.8 if ltv < 95 else 1.0
    dti_score = 0.2 if dti < 36 else 0.5 if dti < 43 \
                else 0.8 if dti < 50 else 1.0
    rate_score = 0.2 if interest_rate < 4 else \
                 0.5 if interest_rate < 6 else \
                 0.8 if interest_rate < 8 else 1.0
    return round(ltv_score*0.30 + dti_score*0.25 + 
                 rate_score*0.25 + macro*0.20, 3)

def get_risk_tier(score):
    if score < 0.3: return "LOW"
    elif score < 0.6: return "MEDIUM"
    elif score < 0.8: return "HIGH"
    else: return "CRITICAL"

class TestRiskScoring:
    def test_low_risk_loan(self):
        score = calculate_risk_score(ltv=70, dti=30, 
                                     interest_rate=3.5)
        assert score < 0.3
        assert get_risk_tier(score) == "LOW"

    def test_critical_risk_loan(self):
        score = calculate_risk_score(ltv=98, dti=55, 
                                     interest_rate=9.0)
        assert score > 0.8
        assert get_risk_tier(score) == "CRITICAL"

    def test_medium_risk_loan(self):
        score = calculate_risk_score(ltv=85, dti=40, 
                                     interest_rate=5.0)
        assert 0.3 <= score < 0.6
        assert get_risk_tier(score) == "MEDIUM"

    def test_high_risk_loan(self):
        score = calculate_risk_score(ltv=92, dti=46, 
                                     interest_rate=7.0)
        assert 0.6 <= score < 0.8
        assert get_risk_tier(score) == "HIGH"

    def test_risk_score_range(self):
        score = calculate_risk_score(ltv=80, dti=36, 
                                     interest_rate=6.0)
        assert 0.0 <= score <= 1.0

    def test_macro_stress_impact(self):
        score_low_macro = calculate_risk_score(
            ltv=85, dti=40, interest_rate=5.0, macro=0.2)
        score_high_macro = calculate_risk_score(
            ltv=85, dti=40, interest_rate=5.0, macro=0.8)
        assert score_high_macro > score_low_macro
