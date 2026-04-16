from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from produce_avro import SOURCE_TOPIC, KEY_SCHEMA, schema_v1


BROKERS = "message_queue:29092"
SCHEMA_REGISTRY_URL = "http://message_queue:8081"
ARTIFACT = Path("/workspace/artifacts/poison-kafka/produce-result.json")


def ts_micros(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000_000)


def profile_value() -> dict[str, Any]:
    return {
        "event_ts": ts_micros("2026-04-07T09:59:00"),
        "customer_id": "c-1999",
        "full_name": "Poison Followup",
        "email": "poison.followup@example.com",
        "status": "ACTIVE",
        "balance": Decimal("9.99"),
        "profile": {
            "age": 30,
            "country": "US",
            "attributes": {
                "segment": "poison-check",
                "country_code": "US",
            },
            "tags": ["poison", "followup"],
            "address": {
                "city": "Seattle",
                "postal_code": "98121",
            },
        },
        "preferred_contact": "email",
        "account_uuid": "67e55044-10b1-426f-9247-bb680e5fe0c8",
    }


@dataclass
class DeliveryRecord:
    label: str
    partition: int
    offset: int
    key: str


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(ARTIFACT))
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    key_serializer = AvroSerializer(schema_registry_client=registry, schema_str=KEY_SCHEMA)
    value_serializer = AvroSerializer(schema_registry_client=registry, schema_str=schema_v1())
    producer = Producer({"bootstrap.servers": BROKERS})

    deliveries: list[DeliveryRecord] = []

    def on_delivery(label: str, key: str):
        def inner(err, msg) -> None:
            if err is not None:
                raise RuntimeError(f"Delivery failed for {label}: {err}")
            deliveries.append(
                DeliveryRecord(
                    label=label,
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=key,
                )
            )

        return inner

    poison_key = "c-poison"
    poison_key_bytes = key_serializer(
        poison_key,
        SerializationContext(SOURCE_TOPIC, MessageField.KEY),
    )
    producer.produce(
        topic=SOURCE_TOPIC,
        key=poison_key_bytes,
        value=b"not-avro-payload",
        on_delivery=on_delivery("poison", poison_key),
    )

    valid_key = "c-1999"
    valid_key_bytes = key_serializer(
        valid_key,
        SerializationContext(SOURCE_TOPIC, MessageField.KEY),
    )
    valid_value_bytes = value_serializer(
        profile_value(),
        SerializationContext(SOURCE_TOPIC, MessageField.VALUE),
    )
    producer.produce(
        topic=SOURCE_TOPIC,
        key=valid_key_bytes,
        value=valid_value_bytes,
        on_delivery=on_delivery("followup_valid", valid_key),
    )

    producer.flush()
    deliveries.sort(key=lambda item: item.offset)

    output.write_text(
        json.dumps({"deliveries": [asdict(item) for item in deliveries]}, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
