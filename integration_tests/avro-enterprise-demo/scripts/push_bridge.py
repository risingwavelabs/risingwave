from __future__ import annotations

import json
import time

from confluent_kafka import Consumer, KafkaException
import requests


TOPIC = "demo.customer.profile.latest"
RECEIVER_URL = "http://push-receiver:8080/endpoint"


def normalize_key(raw: bytes | None) -> str:
    if raw is None:
        return "<null>"
    text = raw.decode("utf-8")
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and "customer_key" in parsed:
            return str(parsed["customer_key"])
        if isinstance(parsed, str):
            return parsed
    except json.JSONDecodeError:
        pass
    return text.strip('"')


def normalize_value(raw: bytes | None) -> dict | None:
    if raw is None:
        return None
    return json.loads(raw.decode("utf-8"))


def post_payload(payload: dict) -> None:
    for _ in range(5):
        response = requests.post(RECEIVER_URL, json=payload, timeout=10)
        if response.status_code < 500:
            response.raise_for_status()
            return
        time.sleep(1)
    response.raise_for_status()


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "message_queue:29092",
            "group.id": "demo-push-bridge",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
    )
    consumer.subscribe([TOPIC])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                error_text = str(message.error())
                if "UNKNOWN_TOPIC_OR_PARTITION" in error_text or "Subscribed topic not available" in error_text:
                    time.sleep(1)
                    continue
                raise KafkaException(message.error())

            value = normalize_value(message.value())
            if value is None:
                continue
            value["customer_key"] = normalize_key(message.key())
            post_payload(value)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
