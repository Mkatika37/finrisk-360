"""
FastAPI — finrisk-360 service layer
Exposes risk indicators and health endpoints.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="finrisk-360 API",
    version="0.1.0",
    description="Financial risk data serving layer",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/api/v1/risk-indicators")
async def get_risk_indicators():
    """Return latest risk indicator data."""
    # TODO: query Snowflake / cache layer
    return {"data": [], "message": "Not yet implemented"}
