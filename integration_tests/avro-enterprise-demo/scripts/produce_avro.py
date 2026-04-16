from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import json
import time
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
import requests


BROKERS = "message_queue:29092"
SCHEMA_REGISTRY_URL = "http://message_queue:8081"
SOURCE_TOPIC = "demo.customer.profile.upsert.avro"
DOWNSTREAM_TOPIC = "demo.customer.profile.latest"

KEY_SCHEMA = '"string"'


def ts_micros(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000_000)


def schema_v1() -> str:
    return json.dumps(
        {
            "type": "record",
            "name": "CustomerProfile",
            "namespace": "demo.avro",
            "fields": [
                {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "customer_id", "type": "string", "default": ""},
                {"name": "full_name", "type": "string"},
                {"name": "email", "type": "string"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "CustomerStatus",
                        "symbols": ["ACTIVE", "DORMANT", "INACTIVE"],
                    },
                },
                {
                    "name": "balance",
                    "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2},
                },
                {
                    "name": "profile",
                    "type": {
                        "type": "record",
                        "name": "Profile",
                        "fields": [
                            {"name": "age", "type": "int"},
                            {"name": "country", "type": "string"},
                            {"name": "attributes", "type": {"type": "map", "values": "string"}},
                            {"name": "tags", "type": {"type": "array", "items": "string"}},
                            {
                                "name": "address",
                                "type": {
                                    "type": "record",
                                    "name": "Address",
                                    "fields": [
                                        {"name": "city", "type": "string"},
                                        {"name": "postal_code", "type": "string"},
                                    ],
                                },
                            },
                        ],
                    },
                },
                {"name": "preferred_contact", "type": ["null", "string", "int"], "default": None},
                {"name": "account_uuid", "type": ["null", {"type": "string", "logicalType": "uuid"}], "default": None},
            ],
        }
    )


def schema_v2() -> str:
    schema = json.loads(schema_v1())
    schema["fields"].append({"name": "vip_note", "type": ["null", "string"], "default": None})
    return json.dumps(schema)


def schema_v3() -> str:
    return schema_v1()


@dataclass
class Step:
    key: str
    value: dict[str, Any] | None


def profile(
    event_ts: str,
    full_name: str,
    email: str,
    status: str,
    balance: str,
    age: int,
    country: str,
    city: str,
    postal_code: str,
    segment: str,
    tags: list[str],
    preferred_contact: str | int | None,
    account_uuid: str | None,
    vip_note: str | None = None,
) -> dict[str, Any]:
    value = {
        "event_ts": ts_micros(event_ts),
        "full_name": full_name,
        "email": email,
        "status": status,
        "balance": Decimal(balance),
        "profile": {
            "age": age,
            "country": country,
            "attributes": {
                "segment": segment,
                "country_code": country,
            },
            "tags": tags,
            "address": {
                "city": city,
                "postal_code": postal_code,
            },
        },
        "preferred_contact": preferred_contact,
        "account_uuid": account_uuid,
    }
    if vip_note is not None:
        value["vip_note"] = vip_note
    return value


STAGE1 = [
    Step(
        "c-1001",
        profile(
            "2026-04-07T09:00:00",
            "Alice Wong",
            "Alice@Example.com",
            "ACTIVE",
            "120.55",
            31,
            "US",
            "Seattle",
            "98101",
            "enterprise",
            ["vip", "northwest"],
            "email",
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
        ),
    ),
    Step(
        "c-1002",
        profile(
            "2026-04-07T09:01:00",
            "Bob Stone",
            "bob@example.com",
            "ACTIVE",
            "75.10",
            28,
            "US",
            "Austin",
            "73301",
            "midmarket",
            ["south"],
            7,
            None,
        ),
    ),
    Step(
        "c-1001",
        profile(
            "2026-04-07T09:02:00",
            "Alice Wong",
            "alice.sales@example.com",
            "DORMANT",
            "180.25",
            31,
            "US",
            "Seattle",
            "98101",
            "enterprise",
            ["vip", "northwest", "reactivation"],
            "sms",
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
        ),
    ),
    Step(
        "c-1003",
        profile(
            "2026-04-07T09:03:00",
            "Carol Reed",
            "carol@example.com",
            "INACTIVE",
            "0.00",
            42,
            "US",
            "Denver",
            "80014",
            "churned",
            ["legacy"],
            None,
            None,
        ),
    ),
    Step("c-1002", None),
    Step(
        "c-1001",
        profile(
            "2026-04-07T09:04:00",
            "Alice Wong",
            "alice.sales@example.com",
            "ACTIVE",
            "205.75",
            31,
            "US",
            "Seattle",
            "98109",
            "enterprise",
            ["vip", "northwest", "reactivation"],
            "email",
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
        ),
    ),
]

STAGE2 = [
    Step(
        "c-1001",
        profile(
            "2026-04-07T09:10:00",
            "Alice Wong",
            "alice.sales@example.com",
            "ACTIVE",
            "225.75",
            31,
            "US",
            "Seattle",
            "98109",
            "enterprise",
            ["vip", "northwest", "reactivation"],
            "email",
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
            vip_note="priority-support",
        ),
    ),
    Step(
        "c-1004",
        profile(
            "2026-04-07T09:11:00",
            "Dan Cruz",
            "dan@example.com",
            "ACTIVE",
            "15.00",
            25,
            "US",
            "Portland",
            "97201",
            "growth",
            ["new", "trial"],
            9,
            None,
            vip_note=None,
        ),
    ),
]

STAGE3 = [
    Step(
        "c-1001",
        profile(
            "2026-04-07T09:20:00",
            "Alice Wong",
            "alice.sales@example.com",
            "ACTIVE",
            "260.10",
            31,
            "US",
            "Seattle",
            "98109",
            "enterprise",
            ["vip", "northwest", "renewed"],
            "email",
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
        ),
    ),
]


def delivery_report(err, msg) -> None:
    if err is not None:
        raise RuntimeError(f"Delivery failed for topic={msg.topic()}: {err}")


def create_topics() -> None:
    client = AdminClient({"bootstrap.servers": BROKERS})
    topics = [
        NewTopic(
            SOURCE_TOPIC,
            num_partitions=1,
            replication_factor=1,
            config={"cleanup.policy": "compact"},
        ),
        NewTopic(
            DOWNSTREAM_TOPIC,
            num_partitions=1,
            replication_factor=1,
            config={"cleanup.policy": "compact"},
        ),
    ]
    futures = client.create_topics(topics)
    for future in futures.values():
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001
            if "Topic already exists" not in str(exc) and "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def delete_topics() -> None:
    client = AdminClient({"bootstrap.servers": BROKERS})
    futures = client.delete_topics([SOURCE_TOPIC, DOWNSTREAM_TOPIC], operation_timeout=30)
    for future in futures.values():
        try:
            future.result()
        except Exception as exc:  # noqa: BLE001
            text = str(exc)
            if "Unknown topic or partition" not in text and "UNKNOWN_TOPIC_OR_PARTITION" not in text:
                raise


def reset_schema_registry() -> None:
    response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=10)
    response.raise_for_status()
    for subject in response.json():
        if not subject.startswith("demo.customer.profile."):
            continue
        delete_response = requests.delete(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}?permanent=true", timeout=10)
        if delete_response.status_code not in (200, 404):
            delete_response.raise_for_status()


def reset_demo_state() -> None:
    delete_topics()
    reset_schema_registry()
    create_topics()


def make_serializers(schema_text: str) -> tuple[AvroSerializer, AvroSerializer]:
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    key_serializer = AvroSerializer(schema_registry_client=registry, schema_str=KEY_SCHEMA)
    value_serializer = AvroSerializer(schema_registry_client=registry, schema_str=schema_text)
    return key_serializer, value_serializer


def produce(schema_text: str, items: list[Step]) -> None:
    key_serializer, value_serializer = make_serializers(schema_text)
    producer = Producer({"bootstrap.servers": BROKERS})
    for item in items:
        payload = None
        if item.value is not None:
            payload = dict(item.value)
            payload["customer_id"] = item.key
        producer.produce(
            topic=SOURCE_TOPIC,
            key=key_serializer(item.key, SerializationContext(SOURCE_TOPIC, MessageField.KEY)),
            value=None
            if payload is None
            else value_serializer(payload, SerializationContext(SOURCE_TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report,
        )
    producer.flush()


def wait_sink_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BROKERS})
    for _ in range(30):
        metadata = admin.list_topics(timeout=5)
        if DOWNSTREAM_TOPIC in metadata.topics:
            return
        time.sleep(1)
    raise RuntimeError(f"Topic {DOWNSTREAM_TOPIC} was not created in time")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--step",
        required=True,
        choices=["setup", "stage1", "schema_v2", "stage2", "schema_v3", "stage3", "wait_sink"],
    )
    args = parser.parse_args()

    if args.step == "setup":
        reset_demo_state()
        make_serializers(schema_v1())
        return
    if args.step == "stage1":
        produce(schema_v1(), STAGE1)
        return
    if args.step == "schema_v2":
        make_serializers(schema_v2())
        return
    if args.step == "stage2":
        produce(schema_v2(), STAGE2)
        return
    if args.step == "schema_v3":
        make_serializers(schema_v3())
        return
    if args.step == "stage3":
        produce(schema_v3(), STAGE3)
        return
    if args.step == "wait_sink":
        wait_sink_topic()
        return


if __name__ == "__main__":
    main()
