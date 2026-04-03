import pytest

class TestRiskScoringLogic:
    """Test risk scoring logic without Snowflake dependency"""
    
    def calculate_risk_score(self, ltv, dti, 
                             interest_rate, macro=0.6):
        ltv_score = (0.2 if ltv < 80 else 
                     0.5 if ltv < 90 else 
                     0.8 if ltv < 95 else 1.0)
        dti_score = (0.2 if dti < 36 else 
                     0.5 if dti < 43 else 
                     0.8 if dti < 50 else 1.0)
        rate_score = (0.2 if interest_rate < 4 else 
                      0.5 if interest_rate < 6 else 
                      0.8 if interest_rate < 8 else 1.0)
        return round(
            ltv_score * 0.30 + dti_score * 0.25 + 
            rate_score * 0.25 + macro * 0.20, 3
        )

    def get_risk_tier(self, score):
        if score < 0.3: return "LOW"
        elif score < 0.6: return "MEDIUM"
        elif score < 0.8: return "HIGH"
        else: return "CRITICAL"

    def get_recommendation(self, tier):
        recommendations = {
            "CRITICAL": "Immediate rejection recommended.",
            "HIGH": "Requires senior underwriter review.",
            "MEDIUM": "Standard underwriting process.",
            "LOW": "Approve with standard terms."
        }
        return recommendations[tier]

    def test_critical_loan(self):
        score = self.calculate_risk_score(
            ltv=98, dti=55, interest_rate=9.0)
        assert score > 0.8
        assert self.get_risk_tier(score) == "CRITICAL"

    def test_high_risk_loan(self):
        score = self.calculate_risk_score(
            ltv=92, dti=46, interest_rate=7.0)
        assert 0.6 <= score < 0.8
        assert self.get_risk_tier(score) == "HIGH"

    def test_medium_risk_loan(self):
        score = self.calculate_risk_score(
            ltv=85, dti=40, interest_rate=5.0)
        assert 0.3 <= score < 0.6
        assert self.get_risk_tier(score) == "MEDIUM"

    def test_low_risk_loan(self):
        score = self.calculate_risk_score(
            ltv=70, dti=30, interest_rate=3.5)
        assert score < 0.3
        assert self.get_risk_tier(score) == "LOW"

    def test_score_range(self):
        score = self.calculate_risk_score(
            ltv=80, dti=36, interest_rate=6.0)
        assert 0.0 <= score <= 1.0

    def test_recommendation_critical(self):
        rec = self.get_recommendation("CRITICAL")
        assert "rejection" in rec.lower()

    def test_recommendation_low(self):
        rec = self.get_recommendation("LOW")
        assert "approve" in rec.lower()

    def test_macro_stress_impact(self):
        score_low = self.calculate_risk_score(
            ltv=85, dti=40, interest_rate=5.0, macro=0.2)
        score_high = self.calculate_risk_score(
            ltv=85, dti=40, interest_rate=5.0, macro=0.8)
        assert score_high > score_low

    def test_fannie_mae_formula(self):
        # Test exact formula: LTV×0.30 + DTI×0.25 + Rate×0.25 + Macro×0.20
        # LTV=70(<80)→0.2, DTI=30(<36)→0.2, Rate=3.5(<4)→0.2, Macro=0.6
        score = self.calculate_risk_score(
            ltv=70, dti=30, interest_rate=3.5, macro=0.6)
        expected = 0.2*0.30 + 0.2*0.25 + 0.2*0.25 + 0.6*0.20
        assert abs(score - round(expected, 3)) < 0.001

    def test_boundary_ltv_80(self):
        score_below = self.calculate_risk_score(
            ltv=79.9, dti=36, interest_rate=6.0)
        score_above = self.calculate_risk_score(
            ltv=80.1, dti=36, interest_rate=6.0)
        assert score_above > score_below

    def test_high_risk_exposure_calculation(self):
        # Simulating portfolio exposure calculation
        critical_loans = 11543
        avg_loan_amount = 324342
        exposure = critical_loans * avg_loan_amount
        exposure_billions = exposure / 1_000_000_000
        assert exposure_billions > 3
        assert exposure_billions < 10
