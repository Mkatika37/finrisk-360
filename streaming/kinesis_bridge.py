import json
import time
import logging
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import ClientError

import os
from dotenv import load_dotenv

load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d'
)

import os
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPICS = ['loan-applications', 'mortgage-rates', 'macro-indicators']
KAFKA_GROUP_ID = 'kinesis-bridge-group'

KINESIS_STREAM_NAME = 'finrisk360-stream'
KINESIS_REGION = 'us-east-1'

def main():
    logging.info("Starting Kafka → Kinesis bridge")
    logging.info(f"Topics: {KAFKA_TOPICS}")
    logging.info(f"Kinesis stream: {KINESIS_STREAM_NAME}")

    # Initialize Kinesis Client
    kinesis_client = boto3.client('kinesis', region_name=KINESIS_REGION)

    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        consumer_timeout_ms=30000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {}
    )

    logging.info("Bridge is live — reading Kafka messages...")

    sent_count = 0
    failed_count = 0

    try:
        for message in consumer:
            topic = message.topic
            record = message.value

            if not isinstance(record, dict):
                record = {"data": record}

            # Add metadata
            record['_source_topic'] = topic
            record['_bridge_ts'] = int(time.time())

            # Determine Partition Key
            partition_key = str(record.get('loan_id', record.get('id', topic)))

            try:
                # Send to Kinesis
                kinesis_client.put_record(
                    StreamName=KINESIS_STREAM_NAME,
                    Data=json.dumps(record),
                    PartitionKey=partition_key
                )
                sent_count += 1
            except ClientError as e:
                logging.error(f"Failed to send record to Kinesis: {e}")
                failed_count += 1
            except Exception as e:
                logging.error(f"Unexpected error sending to Kinesis: {e}")
                failed_count += 1

            # Log progress every 100 records
            if (sent_count + failed_count) % 100 == 0:
                logging.info(f"Progress → sent: {sent_count} | failed: {failed_count} | topic: {topic}")

    except KeyboardInterrupt:
        pass  # Graceful exit
    finally:
        consumer.close()
        logging.info(f"Total sent: {sent_count} | Failed: {failed_count}")

if __name__ == "__main__":
    main()
