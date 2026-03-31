"""
Kafka Consumer
Consumes messages from Kafka topics and forwards them
to the Snowflake raw landing zone.
"""

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def consume_messages(topic: str) -> None:
    """Consume and process messages from a Kafka topic."""
    # TODO: implement Kafka consumer loop
    raise NotImplementedError


if __name__ == "__main__":
    consume_messages("fred_raw")
