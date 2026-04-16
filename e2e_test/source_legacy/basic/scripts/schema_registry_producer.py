from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import (
    record_subject_name_strategy,
    topic_record_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import json
import os
import sys
from pathlib import Path

for parent in Path(__file__).resolve().parents:
    if (parent / "e2e_test").is_dir():
        parent_str = str(parent)
        if parent_str not in sys.path:
            sys.path.insert(0, parent_str)
        break

from e2e_test.py_utils.kafka import (  # noqa: E402
    create_kafka_producer,
    create_schema_registry_client,
    produce_serialized_messages,
)


def create_topic(kafka_conf, topic_name):
    client = AdminClient(kafka_conf)
    topic_list = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
    client.create_topics(topic_list)


def load_avro_json(encoded, schema):
    """Unlike `json.loads`, this decodes a json according to given avro schema.

    https://avro.apache.org/docs/1.11.1/specification/#json-encoding
    Specially, it handles `union` variants, and differentiates `bytes` from `string`.
    """
    from fastavro import json_reader
    from io import StringIO

    with StringIO(encoded) as buf:
        reader = json_reader(buf, schema)
        return next(reader)


def iter_serialized_messages(
    file_path,
    schema_type,
    schema_registry_client,
    avro_ser_conf,
    topic,
):
    key_serializer = None
    value_serializer = None
    key_schema = None
    value_schema = None

    with open(file_path) as source_file:
        for (i, line) in enumerate(source_file):
            parts = line.split("^")
            if i == 0:
                if len(parts) > 1:
                    if schema_type == "avro":
                        key_serializer = AvroSerializer(
                            schema_registry_client=schema_registry_client,
                            schema_str=parts[0],
                            conf=avro_ser_conf,
                        )
                        value_serializer = AvroSerializer(
                            schema_registry_client=schema_registry_client,
                            schema_str=parts[1],
                            conf=avro_ser_conf,
                        )
                        key_schema = json.loads(parts[0])
                        value_schema = json.loads(parts[1])
                    else:
                        key_serializer = JSONSerializer(
                            schema_registry_client=schema_registry_client,
                            schema_str=parts[0],
                        )
                        value_serializer = JSONSerializer(
                            schema_registry_client=schema_registry_client,
                            schema_str=parts[1],
                        )
                else:
                    if schema_type == "avro":
                        value_serializer = AvroSerializer(
                            schema_registry_client=schema_registry_client,
                            schema_str=parts[0],
                        )
                        value_schema = json.loads(parts[0])
                    else:
                        value_serializer = JSONSerializer(
                            schema_registry_client=schema_registry_client,
                            schema_str=parts[0],
                        )
                continue

            key_idx = None
            value_idx = None
            if len(parts) > 1:
                key_idx = 0
                if len(parts[1].strip()) > 0:
                    value_idx = 1
            else:
                value_idx = 0

            if schema_type == "avro":
                if key_idx is not None:
                    key_json = load_avro_json(parts[key_idx], key_schema)
                if value_idx is not None:
                    value_json = load_avro_json(parts[value_idx], value_schema)
            else:
                if key_idx is not None:
                    key_json = json.loads(parts[key_idx])
                if value_idx is not None:
                    value_json = json.loads(parts[value_idx])

            message = {}
            if key_idx is not None:
                message["key"] = key_serializer(
                    key_json,
                    SerializationContext(topic, MessageField.KEY),
                )
            if value_idx is not None:
                message["value"] = value_serializer(
                    value_json,
                    SerializationContext(topic, MessageField.VALUE),
                )
            yield message


if __name__ == '__main__':
    if len(sys.argv) <= 5:
        print(
            "usage: schema_registry_producer.py <brokerlist> <schema-registry-url> <file> <name-strategy> <json/avro>"
        )
        exit(1)
    broker_list = sys.argv[1]
    schema_registry_url = sys.argv[2]
    file_path = sys.argv[3]
    topic = os.path.basename(file_path).split(".")[0]
    name_strategy = sys.argv[4]
    schema_type = sys.argv[5]
    if name_strategy not in ['topic', 'record', 'topic-record', '', None]:
        print("name strategy must be one of: topic, record, topic_record")
        exit(1)
    if name_strategy != 'topic':
        topic = topic + "-" + name_strategy

    print("broker_list: {}".format(broker_list))
    print("schema_registry_url: {}".format(schema_registry_url))
    print("topic: {}".format(topic))
    print("name strategy: {}".format(
        name_strategy if name_strategy is not None else 'topic'))
    print("type: {}".format(schema_type))

    schema_registry_conf = {'url': schema_registry_url}
    kafka_conf = {'bootstrap.servers': broker_list}
    schema_registry_client = create_schema_registry_client(schema_registry_conf)

    avro_ser_conf = dict()
    if name_strategy == 'record':
        avro_ser_conf['subject.name.strategy'] = record_subject_name_strategy
    elif name_strategy == 'topic-record':
        avro_ser_conf['subject.name.strategy'] = topic_record_subject_name_strategy

    create_topic(kafka_conf=kafka_conf, topic_name=topic)
    producer = create_kafka_producer(kafka_conf)
    produce_serialized_messages(
        producer,
        topic,
        iter_serialized_messages(
            file_path,
            schema_type,
            schema_registry_client,
            avro_ser_conf,
            topic,
        ),
    )
