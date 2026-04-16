from __future__ import annotations

import json
from pathlib import Path
import time

from confluent_kafka import Consumer, KafkaException


OUTPUT = Path("/workspace/artifacts/downstream/latest_state.json")
TOPIC = "demo.customer.profile.latest"


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


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "message_queue:29092",
            "group.id": f"demo-latest-state-reader-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
    )
    state: dict[str, dict | None] = {}
    consumer.subscribe([TOPIC])
    empty_polls = 0
    try:
        while empty_polls < 5:
            message = consumer.poll(1.0)
            if message is None:
                empty_polls += 1
                continue
            if message.error():
                raise KafkaException(message.error())
            empty_polls = 0
            state[normalize_key(message.key())] = normalize_value(message.value())
    finally:
        consumer.close()

    OUTPUT.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
