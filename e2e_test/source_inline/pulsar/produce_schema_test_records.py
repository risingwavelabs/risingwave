#!/usr/bin/env python3

import argparse
import json
import os
import sys

import pulsar
from pulsar.schema import AvroSchema, Integer, JsonSchema, Record, String


def record_type(version):
    fields = {"id": Integer(required=True), "name": String(required=True)}
    if version == 1:
        fields["category"] = String(
            required=True, default="unknown", required_default=True
        )
    # Both versions must have the same Avro record name for schema resolution.
    return type("SchemaTestRecord", (Record,), fields)


def main():
    parser = argparse.ArgumentParser(
        description="Produce records for the Pulsar Schema Registry source test"
    )
    parser.add_argument(
        "--broker",
        default=os.environ.get("PULSAR_BROKER_URL", "pulsar://localhost:6650"),
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--schema-type", choices=("avro", "json"), default="avro")
    parser.add_argument("--schema-version", type=int, choices=(0, 1), default=0)
    args = parser.parse_args()

    client = pulsar.Client(args.broker)
    record = record_type(args.schema_version)
    schema = AvroSchema(record) if args.schema_type == "avro" else JsonSchema(record)
    producer = client.create_producer(args.topic, schema=schema)
    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            value = json.loads(line)
            producer.send(record(**value))
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
