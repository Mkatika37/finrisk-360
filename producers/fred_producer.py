"""
FRED Producer — FinRisk 360
Fetches economic indicators and interest rates from FRED API
and publishes them to Kafka.
"""

import json
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from fredapi import Fred
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration ────────────────────────────────────────────
load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "mortgage-rates"

SERIES_MAP = {
    "MORTGAGE30US": "30-Year Fixed Mortgage Rate",
    "MORTGAGE15US": "15-Year Fixed Mortgage Rate",
    "DRSFRMACBS": "Mortgage Delinquency Rate",
    "DRCCLACBS": "Credit Card Delinquency Rate",
    "DGS10": "10-Year Treasury Yield"
}

OBSERVATION_START = "2020-01-01"

def connect_kafka() -> KafkaProducer:
    """Connect to Kafka and return a KafkaProducer."""
    try:
        KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except NoBrokersAvailable as exc:
        print(f"Kafka connection error: {exc}")
        sys.exit(1)


def run() -> None:
    """Main pipeline to fetch data from FRED and publish to Kafka."""
    if not FRED_API_KEY:
        print("Error: FRED_API_KEY environment variable is missing.")
        sys.exit(1)

    fred = Fred(api_key=FRED_API_KEY)
    producer = connect_kafka()
    
    total_published = 0

    try:
        for series_id, label in SERIES_MAP.items():
            print(f"Fetching {series_id} ({label}) since {OBSERVATION_START}...")
            data = fred.get_series(series_id, observation_start=OBSERVATION_START)
            
            previous_value = None
            
            for date, value in data.items():
                if pd.isna(value):
                    continue
                
                # Calculate rate change in basis points
                if previous_value is None:
                    rate_change_bps = 0.0
                else:
                    rate_change_bps = (value - previous_value) * 100
                    
                record = {
                    "series_id": series_id,
                    "label": label,
                    "date": date.strftime("%Y-%m-%d"),
                    "value": float(value),
                    "rate_change_bps": float(rate_change_bps),
                    "event_type": "mortgage_rate",
                    "ingestion_timestamp": datetime.now().isoformat()
                }
                
                producer.send(KAFKA_TOPIC, value=record)
                total_published += 1
                previous_value = value
                
        producer.flush()
        
    except Exception as exc:
        print(f"An error occurred: {exc}")
        sys.exit(1)
    finally:
        producer.close()

    print(f"Complete: Published {total_published} total rate observations")


if __name__ == "__main__":
    run()
