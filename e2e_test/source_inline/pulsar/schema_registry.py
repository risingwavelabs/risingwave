#!/usr/bin/env python3

import argparse
import json
import os
import sys

import pulsar
from pulsar.schema import AvroSchema, Integer, JsonSchema, Record, String


class TestRecord(Record):
    id = Integer(required=True)
    name = String(required=True)


def main():
    parser = argparse.ArgumentParser(
        description="Produce schema-aware records with Pulsar Schema Registry"
    )
    parser.add_argument(
        "--broker",
        default=os.environ.get("PULSAR_BROKER_URL", "pulsar://localhost:6650"),
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--schema-type", choices=("avro", "json"), default="avro")
    args = parser.parse_args()

    client = pulsar.Client(args.broker)
    schema = AvroSchema(TestRecord) if args.schema_type == "avro" else JsonSchema(TestRecord)
    producer = client.create_producer(args.topic, schema=schema)
    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            value = json.loads(line)
            producer.send(TestRecord(id=value["id"], name=value["name"]))
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
