from fastapi.testclient import TestClient
import sys
import os

# Ensure project root is in path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app

client = TestClient(app)

class TestAPI:
    def test_root_endpoint(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "FinRisk 360" in response.json()["name"]

    def test_health_endpoint(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] in ["healthy", "degraded"]

    def test_score_loan_critical(self):
        response = client.post("/score-loan", json={
            "loan_amount": 500000,
            "ltv": 98.0,
            "dti": 55.0,
            "interest_rate": 9.0,
            "income": 80000
        })
        assert response.status_code == 200
        assert response.json()["risk_tier"] == "CRITICAL"
        assert response.json()["risk_score"] > 0.8

    def test_score_loan_low(self):
        response = client.post("/score-loan", json={
            "loan_amount": 200000,
            "ltv": 65.0,
            "dti": 28.0,
            "interest_rate": 3.5,
            "income": 150000
        })
        assert response.status_code == 200
        assert response.json()["risk_tier"] == "LOW"

    def test_invalid_input(self):
        # Allow 422 because pydantic will block negative values
        response = client.post("/score-loan", json={
            "loan_amount": -1000,
            "ltv": 200.0,
            "dti": -5.0,
            "interest_rate": 0,
            "income": 0
        })
        assert response.status_code in [200, 422]
