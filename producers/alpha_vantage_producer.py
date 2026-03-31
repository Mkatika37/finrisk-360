"""
Alpha Vantage Producer — FinRisk 360
Fetches macro indicators from Alpha Vantage API and publishes them to Kafka
as daily snapshots.
"""

import json
import os
import sys
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration ────────────────────────────────────────────
load_dotenv()

ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "macro-indicators"

BASE_URL = "https://www.alphavantage.co/query"
OBSERVATION_START = "2020-01-01"

INDICATORS = [
    {
        "name": "Federal Funds Rate",
        "key": "fed_funds_rate",
        "params": {
            "function": "FEDERAL_FUNDS_RATE",
            "interval": "monthly"
        }
    },
    {
        "name": "Treasury Yield",
        "key": "treasury_yield_10yr",
        "params": {
            "function": "TREASURY_YIELD",
            "interval": "monthly",
            "maturity": "10year"
        }
    },
    {
        "name": "Unemployment Rate",
        "key": "unemployment_rate",
        "params": {
            "function": "UNEMPLOYMENT"
        }
    },
    {
        "name": "CPI",
        "key": "cpi",
        "params": {
            "function": "CPI",
            "interval": "monthly"
        }
    }
]


def connect_kafka() -> KafkaProducer:
    """Connect to Kafka and return a KafkaProducer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except NoBrokersAvailable as exc:
        print(f"Kafka connection error: {exc}")
        sys.exit(1)


def run() -> None:
    """Main pipeline: fetch 4 indicators, merge by date, publish to Kafka."""
    if not ALPHA_VANTAGE_KEY:
        print("Error: ALPHA_VANTAGE_KEY environment variable is missing.")
        sys.exit(1)

    combined_data = {}

    for i, ind in enumerate(INDICATORS):
        print(f"Fetching {ind['name']}...")
        
        params = ind["params"].copy()
        params["apikey"] = ALPHA_VANTAGE_KEY
        
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as exc:
            print(f"HTTP Error fetching {ind['name']}: {exc}")
            sys.exit(1)
            
        for record in data.get("data", []):
            date_str = record.get("date")
            val_str = record.get("value")
            
            if not date_str or date_str < OBSERVATION_START:
                continue
                
            if val_str == ".":
                continue
                
            if date_str not in combined_data:
                combined_data[date_str] = {}
                
            combined_data[date_str][ind["key"]] = float(val_str)
            
        print(f"Successfully processed {ind['name']}.")
            
        if i < len(INDICATORS) - 1:
            print("Waiting 15 seconds before next API call...")
            time.sleep(15)

    print("All indicators fetched. Publishing to Kafka...")
    
    producer = connect_kafka()
    published_count = 0
    
    try:
        # Sort dates for chronological publishing
        for date_str in sorted(combined_data.keys()):
            row = combined_data[date_str]
            
            fed_rate = row.get("fed_funds_rate")
            tsy_yield = row.get("treasury_yield_10yr")
            unemp = row.get("unemployment_rate")
            cpi = row.get("cpi")
            
            # Skip records that don't have all 4 required indicators for the snapshot
            if fed_rate is None or tsy_yield is None or unemp is None or cpi is None:
                continue
                
            # macro_stress_index formula:
            # (fed_funds_rate/10 * 0.40) + (unemployment_rate/10 * 0.35) + (cpi/500 * 0.25)
            msi_raw = (fed_rate / 10.0 * 0.40) + (unemp / 10.0 * 0.35) + (cpi / 500.0 * 0.25)
            msi_clamped = max(0.0, min(1.0, msi_raw))
            
            record = {
                "date": date_str,
                "fed_funds_rate": fed_rate,
                "treasury_yield_10yr": tsy_yield,
                "unemployment_rate": unemp,
                "cpi": cpi,
                "macro_stress_index": msi_clamped,
                "event_type": "macro_indicator",
                "ingestion_timestamp": datetime.now().isoformat()
            }
            
            producer.send(KAFKA_TOPIC, value=record)
            published_count += 1
            
        producer.flush()
        
    except Exception as exc:
        print(f"An error occurred: {exc}")
        sys.exit(1)
    finally:
        producer.close()

    print(f"Complete: Published {published_count} macro snapshots")


if __name__ == "__main__":
    run()
