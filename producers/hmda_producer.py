"""
HMDA Producer — FinRisk 360
Fetches 2024 loan application data from the CFPB HMDA Data Browser API,
saves it locally, parses CSV rows, and publishes enriched records to Kafka.
"""

import csv
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

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "loan-applications"

HMDA_URL = (
    "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
    "?states=VA,MD,DC&years=2024&loan_types=1,2"
)

LOCAL_FILE_PATH = "data/hmda_2024_raw.csv"

# Rate limiting
SLEEP_INTERVAL = 0.01  # 100 events per second
PROGRESS_INTERVAL = 1000

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

def download_file_if_needed():
    """Download HMDA data and save it to a local file in chunks."""
    os.makedirs("data", exist_ok=True)
    
    if os.path.exists(LOCAL_FILE_PATH):
        print("Found existing file, skipping download")
        return
        
    print(f"Downloading HMDA CSV from {HMDA_URL}")
    response = requests.get(HMDA_URL, stream=True, timeout=30)
    response.raise_for_status()
    
    bytes_downloaded = 0
    five_mb = 5 * 1024 * 1024
    last_reported_mb = 0

    with open(LOCAL_FILE_PATH, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                bytes_downloaded += len(chunk)
                
                current_mb = bytes_downloaded // five_mb * 5
                if current_mb > last_reported_mb:
                    print(f"Downloading... {current_mb}MB")
                    last_reported_mb = current_mb
                    
    print("Download complete.")

def process_and_publish(producer: KafkaProducer):
    """Read the local CSV file and publish rows to Kafka."""
    print("Opening local file to process records...")
    
    published = 0
    
    with open(LOCAL_FILE_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            loan_amount = row.get("loan_amount", "").strip()
            ltv = row.get("loan_to_value_ratio", "").strip()
            
            # Skip rows where loan_amount or ltv is empty string
            if loan_amount == "" or ltv == "":
                continue
                
            record = {
                "loan_amount": loan_amount,
                "loan_type": row.get("loan_type", ""),
                "action_taken": row.get("action_taken", ""),
                "ltv": ltv,
                "dti": row.get("debt_to_income_ratio", ""),
                "income": row.get("income", ""),
                "interest_rate": row.get("interest_rate", ""),
                "denial_reason_1": row.get("denial_reason_1", ""),
                "state": row.get("state_code", ""),
                "county_code": row.get("county_code", ""),
                "property_value": row.get("property_value", ""),
                "event_type": "loan_application",
                "ingestion_timestamp": datetime.now().isoformat()
            }
            
            producer.send(KAFKA_TOPIC, value=record)
            published += 1
            
            if published % PROGRESS_INTERVAL == 0:
                print(f"Published {published} records to {KAFKA_TOPIC}")
                
            time.sleep(SLEEP_INTERVAL)
            
    producer.flush()
    print(f"Complete: Published {published} total records")

def run() -> None:
    """Main pipeline: download → read local → parse → publish."""
    download_file_if_needed()
    
    producer = connect_kafka()
    try:
        process_and_publish(producer)
    finally:
        producer.close()

if __name__ == "__main__":
    run()
