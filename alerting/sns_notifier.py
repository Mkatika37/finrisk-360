"""
SNS Notifier
Sends pipeline alerts to an AWS SNS topic.
"""

import json
import os

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def send_sns_alert(subject: str, message: str) -> str:
    """Publish an alert message to AWS SNS. Returns MessageId."""
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 is required — pip install boto3")

    if not SNS_TOPIC_ARN:
        raise ValueError("SNS_TOPIC_ARN not set")

    client = boto3.client("sns", region_name=AWS_REGION)
    response = client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=json.dumps({"default": message}),
        MessageStructure="json",
    )
    return response["MessageId"]


if __name__ == "__main__":
    mid = send_sns_alert(
        subject="finrisk-360 Pipeline Alert",
        message="Test alert from finrisk-360 pipeline",
    )
    print(f"Published SNS message: {mid}")
