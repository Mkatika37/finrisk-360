import os
import time
import logging
from datetime import datetime
from typing import List, Optional

import snowflake.connector
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator

from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Gauge
import threading

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# App init
# ─────────────────────────────────────────────
app = FastAPI(
    title="FinRisk 360 API",
    description="Real-time mortgage risk scoring powered by Fannie Mae DU formula",
    version="1.0.0",
)

# Instrument for metrics
Instrumentator().instrument(app).expose(app)

# Custom metrics for Grafana
risk_gauge = Gauge('risk_tier_count', 'Current loan count by risk tier', ['risk_tier'])
kafka_gauge = Gauge('kafka_topic_offset', 'Kafka topic cumulative offset', ['topic'])

# ─────────────────────────────────────────────
# Kafka monitor thread
# ─────────────────────────────────────────────
def monitor_kafka():
    from kafka import KafkaConsumer, TopicPartition
    logger.info("Starting internal Kafka monitor thread...")
    try:
        consumer = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
        while True:
            try:
                topics = consumer.topics()
                for topic in topics:
                    if topic.startswith('_'): continue
                    partitions = consumer.partitions_for_topic(topic)
                    if not partitions: continue
                    tps = [TopicPartition(topic, p) for p in partitions]
                    end_offsets = consumer.end_offsets(tps)
                    total = sum(end_offsets.values())
                    kafka_gauge.labels(topic=topic).set(total)
            except Exception as e:
                logger.warning(f"Kafka monitor poll error: {e}")
            time.sleep(15)
    except Exception as e:
        logger.error(f"Kafka monitor thread failed: {e}")

threading.Thread(target=monitor_kafka, daemon=True).start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# Snowflake helper
# ─────────────────────────────────────────────
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "FINRISK360"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "FINRISK_WH"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    )

def snowflake_ok() -> bool:
    try:
        conn = get_snowflake_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False

# ─────────────────────────────────────────────
# Scoring logic
# ─────────────────────────────────────────────
RECOMMENDATIONS = {
    "CRITICAL": "Immediate rejection recommended. Exceeds all risk thresholds.",
    "HIGH":     "Loan requires senior underwriter review. High default probability.",
    "MEDIUM":   "Standard underwriting process. Monitor LTV and DTI closely.",
    "LOW":      "Approve with standard terms. Low default risk.",
}

def compute_risk(ltv: float, dti: float, interest_rate: float) -> dict:
    ltv_score   = 0.2 if ltv < 80  else 0.5 if ltv < 90  else 0.8 if ltv < 95  else 1.0
    dti_score   = 0.2 if dti < 36  else 0.5 if dti < 43  else 0.8 if dti < 50  else 1.0
    rate_score  = 0.2 if interest_rate < 4 else 0.5 if interest_rate < 6 else 0.8 if interest_rate < 8 else 1.0
    macro_stress = 0.6

    risk_score = round(
        ltv_score * 0.30 + dti_score * 0.25 + rate_score * 0.25 + macro_stress * 0.20, 4
    )

    if risk_score < 0.3:
        tier = "LOW"
    elif risk_score < 0.6:
        tier = "MEDIUM"
    elif risk_score < 0.85:
        tier = "HIGH"
    else:
        tier = "CRITICAL"

    return {
        "ltv_score": ltv_score,
        "dti_score": dti_score,
        "rate_spread_score": rate_score,
        "macro_stress_score": macro_stress,
        "risk_score": risk_score,
        "risk_tier": tier,
        "recommendation": RECOMMENDATIONS[tier],
    }

# ─────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────
class LoanInput(BaseModel):
    loan_amount: float = Field(..., gt=0, le=10_000_000, example=450000)
    ltv: float         = Field(..., ge=0, le=200,         example=95.0)
    dti: float         = Field(..., ge=0, le=100,         example=48.0)
    interest_rate: float = Field(..., ge=0, le=30,        example=8.5)
    income: float      = Field(..., ge=0,                 example=85000)

    @validator("loan_amount", "income")
    def must_be_positive(cls, v):
        if v < 0:
            raise ValueError("Value must be positive")
        return v

class LoanScore(LoanInput):
    ltv_score: float
    dti_score: float
    rate_spread_score: float
    macro_stress_score: float
    risk_score: float
    risk_tier: str
    recommendation: str
    scored_at: str
    response_time_ms: int

class BatchInput(BaseModel):
    loans: List[LoanInput] = Field(..., max_items=100)

class CriticalLoan(BaseModel):
    risk_score: float
    risk_tier: str
    loan_amount: float
    ltv: Optional[float]
    dti: Optional[float]
    interest_rate: Optional[float]

class CriticalLoansResponse(BaseModel):
    count: int
    loans: List[CriticalLoan]

# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────
@app.get("/", tags=["General"])
def root():
    logger.info("GET /")
    return {
        "name": "FinRisk 360 API",
        "description": "Real-time mortgage risk scoring",
        "version": "1.0.0",
        "endpoints": {
            "health":        "GET  /health",
            "score_loan":    "POST /score-loan",
            "batch_score":   "POST /score-batch",
            "stats":         "GET  /stats",
            "critical_loans":"GET  /loans/critical",
        },
        "docs": "http://localhost:8000/docs",
    }


@app.get("/health", tags=["General"])
def health():
    logger.info("GET /health")
    sf_ok = snowflake_ok()
    total = 0
    if sf_ok:
        try:
            conn = get_snowflake_conn()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM LOAN_RISK_SCORES")
            total = cur.fetchone()[0]
            conn.close()
        except Exception as e:
            logger.warning(f"Health count query failed: {e}")

    return {
        "status": "healthy" if sf_ok else "degraded",
        "service": "FinRisk 360 API",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "snowflake_connected": sf_ok,
        "total_loans_scored": total,
    }


@app.post("/score-loan", response_model=LoanScore, tags=["Scoring"])
def score_loan(loan: LoanInput):
    logger.info(f"POST /score-loan | ltv={loan.ltv} dti={loan.dti} rate={loan.interest_rate}")
    t0 = time.time()
    result = compute_risk(loan.ltv, loan.dti, loan.interest_rate)
    elapsed_ms = int((time.time() - t0) * 1000)

    # Track risk distribution for Grafana
    risk_gauge.labels(risk_tier=result['risk_tier']).inc()

    return LoanScore(
        **loan.dict(),
        **result,
        scored_at=datetime.utcnow().isoformat(),
        response_time_ms=elapsed_ms,
    )


@app.post("/score-batch", tags=["Scoring"])
def score_batch(batch: BatchInput):
    logger.info(f"POST /score-batch | count={len(batch.loans)}")
    results = []
    for loan in batch.loans:
        t0 = time.time()
        result = compute_risk(loan.ltv, loan.dti, loan.interest_rate)
        elapsed_ms = int((time.time() - t0) * 1000)
        
        # Track risk distribution for Grafana
        risk_gauge.labels(risk_tier=result['risk_tier']).inc()

        results.append({
            **loan.dict(),
            **result,
            "scored_at": datetime.utcnow().isoformat(),
            "response_time_ms": elapsed_ms,
        })
    return {"scored": len(results), "results": results}


@app.get("/stats", tags=["Analytics"])
def stats():
    logger.info("GET /stats")
    try:
        conn = get_snowflake_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM LOAN_RISK_SCORES")
        total = cur.fetchone()[0]

        cur.execute("SELECT risk_tier, COUNT(*) FROM LOAN_RISK_SCORES GROUP BY risk_tier")
        dist = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT AVG(risk_score), AVG(loan_amount) FROM LOAN_RISK_SCORES")
        row = cur.fetchone()
        avg_risk = round(float(row[0]), 3) if row[0] else 0.0
        avg_loan = round(float(row[1]), 0) if row[1] else 0.0

        cur.execute(
            "SELECT SUM(loan_amount) FROM LOAN_RISK_SCORES "
            "WHERE risk_tier IN ('CRITICAL','HIGH')"
        )
        exposure = cur.fetchone()[0] or 0
        exposure_b = round(exposure / 1e9, 2)

        cur.execute("SELECT MAX(processing_date) FROM LOAN_RISK_SCORES")
        last_updated = str(cur.fetchone()[0])

        conn.close()
        return {
            "total_loans": total,
            "risk_distribution": dist,
            "avg_risk_score": avg_risk,
            "avg_loan_amount": avg_loan,
            "high_risk_exposure_billions": exposure_b,
            "last_updated": last_updated,
        }
    except Exception as e:
        logger.error(f"Stats query failed: {e}")
        raise HTTPException(status_code=503, detail=f"Snowflake unavailable: {e}")


@app.get("/loans/critical", response_model=CriticalLoansResponse, tags=["Analytics"])
def critical_loans(limit: int = Query(default=10, ge=1, le=100)):
    logger.info(f"GET /loans/critical | limit={limit}")
    try:
        conn = get_snowflake_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT risk_score, risk_tier, loan_amount, ltv, dti, interest_rate
            FROM LOAN_RISK_SCORES
            WHERE risk_tier = 'CRITICAL'
            ORDER BY risk_score DESC
            LIMIT {limit}
            """
        )
        rows = cur.fetchall()
        conn.close()
        loans = [
            CriticalLoan(
                risk_score=r[0], risk_tier=r[1], loan_amount=r[2],
                ltv=r[3], dti=r[4], interest_rate=r[5]
            )
            for r in rows
        ]
        return CriticalLoansResponse(count=len(loans), loans=loans)
    except Exception as e:
        logger.error(f"Critical loans query failed: {e}")
        raise HTTPException(status_code=503, detail=f"Snowflake unavailable: {e}")
