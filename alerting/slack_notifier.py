"""
Slack Notifier
Sends pipeline alerts to a Slack channel via incoming webhook.
"""

import json
import os
import urllib.request

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def send_slack_alert(message: str, severity: str = "warning") -> None:
    """Post an alert message to Slack."""
    if not SLACK_WEBHOOK_URL:
        raise ValueError("SLACK_WEBHOOK_URL not set")

    color_map = {"info": "#36a64f", "warning": "#ff9900", "critical": "#ff0000"}
    payload = {
        "attachments": [
            {
                "color": color_map.get(severity, "#ff9900"),
                "title": f"finrisk-360 Alert [{severity.upper()}]",
                "text": message,
            }
        ]
    }

    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req)


if __name__ == "__main__":
    send_slack_alert("Test alert from finrisk-360 pipeline", severity="info")
