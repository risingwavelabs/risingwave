#!/usr/bin/env python3

import argparse
import base64
import json
import os
import sys

import _pulsar
import pulsar
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pulsar.schema import AvroSchema, Integer, JsonSchema, Record, Schema, String


def record_type(version):
    fields = {"id": Integer(required=True), "name": String(required=True)}
    if version == 1:
        fields["category"] = String(
            required=True, default="unknown", required_default=True
        )
    # Both versions must have the same Avro record name for schema resolution.
    return type("SchemaTestRecord", (Record,), fields)


class ProtobufNativeSchema(Schema):
    def __init__(self, version):
        file = descriptor_pb2.FileDescriptorProto(
            name="schema_test.proto", package="test", syntax="proto3"
        )
        message = file.message_type.add(name="SchemaTestRecord")
        field = descriptor_pb2.FieldDescriptorProto
        message.field.add(name="id", number=1, type=field.TYPE_INT32)
        message.field.add(name="name", number=2, type=field.TYPE_STRING)
        if version == 1:
            message.field.add(name="category", number=3, type=field.TYPE_STRING)

        pool = descriptor_pool.DescriptorPool()
        self._message_cls = message_factory.GetMessageClass(
            pool.Add(file).message_types_by_name["SchemaTestRecord"]
        )
        file_descriptor_set = descriptor_pb2.FileDescriptorSet(file=[file])
        super().__init__(
            self._message_cls,
            _pulsar.SchemaType.PROTOBUF_NATIVE,
            {
                "fileDescriptorSet": base64.b64encode(
                    file_descriptor_set.SerializeToString()
                ).decode(),
                "rootMessageTypeName": "test.SchemaTestRecord",
                "rootFileDescriptorName": file.name,
            },
            "PROTOBUF_NATIVE",
        )

    def record(self, **value):
        return self._message_cls(**value)

    def encode(self, obj):
        return obj.SerializeToString()

    def decode(self, data):
        return self._message_cls.FromString(data)


def main():
    parser = argparse.ArgumentParser(
        description="Produce records for the Pulsar Schema Registry source test"
    )
    parser.add_argument(
        "--broker",
        default=os.environ.get("PULSAR_BROKER_URL", "pulsar://localhost:6650"),
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument(
        "--schema-type", choices=("avro", "json", "protobuf_native"), default="avro"
    )
    parser.add_argument("--schema-version", type=int, choices=(0, 1), default=0)
    args = parser.parse_args()

    client = pulsar.Client(args.broker)
    if args.schema_type == "protobuf_native":
        schema = ProtobufNativeSchema(args.schema_version)
        record = schema.record
    else:
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
