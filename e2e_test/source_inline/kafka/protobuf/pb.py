import json
import importlib
import sys
from pathlib import Path

from google.protobuf.source_context_pb2 import SourceContext
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

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


def get_user(i):
    return user_pb2.User(
        id=i,
        name="User_{}".format(i),
        address="Address_{}".format(i),
        city="City_{}".format(i),
        gender=user_pb2.MALE if i % 2 == 0 else user_pb2.FEMALE,
        sc=SourceContext(file_name="source/context_{:03}.proto".format(i)),
    )


def get_user_with_more_fields(i):
    return user_pb2.User(
        id=i,
        name="User_{}".format(i),
        address="Address_{}".format(i),
        city="City_{}".format(i),
        gender=user_pb2.MALE if i % 2 == 0 else user_pb2.FEMALE,
        sc=SourceContext(file_name="source/context_{:03}.proto".format(i)),
        age=100 + i,
    )


def send_to_kafka(
    producer_conf, schema_registry_conf, topic, num_records, get_user_fn, pb_message
):
    schema_registry_client = create_schema_registry_client(schema_registry_conf)
    serializer = ProtobufSerializer(
        pb_message,
        schema_registry_client,
        {"use.deprecated.format": False, "skip.known.types": True},
    )

    producer = create_kafka_producer(producer_conf)

    def build_messages():
        for i in range(num_records):
            user = get_user_fn(i)
            yield {
                # RisingWave does not handle key schema, so we use json.
                "key": json.dumps({"id": i}),
                "value": serializer(
                    user,
                    SerializationContext(topic, MessageField.VALUE),
                ),
            }

    produce_serialized_messages(producer, topic, build_messages())
    print("Send {} records to kafka\n".format(num_records))


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print(
            "pb.py <brokerlist> <schema-registry-url> <topic> <num-records> <pb_message>"
        )
        exit(1)

    broker_list = sys.argv[1]
    schema_registry_url = sys.argv[2]
    topic = sys.argv[3]
    num_records = int(sys.argv[4])
    pb_message = sys.argv[5]

    user_pb2 = importlib.import_module(f"{pb_message}_pb2")

    all_pb_messages = {
        "user": get_user,
        "user_with_more_fields": get_user_with_more_fields,
    }

    assert (
        pb_message in all_pb_messages
    ), f"pb_message must be one of {list(all_pb_messages.keys())}"

    schema_registry_conf = {"url": schema_registry_url}
    producer_conf = {"bootstrap.servers": broker_list}

    try:
        send_to_kafka(
            producer_conf,
            schema_registry_conf,
            topic,
            num_records,
            all_pb_messages[pb_message],
            user_pb2.User,
        )
    except Exception as e:
        print("Send Protobuf data to schema registry and kafka failed {}", e)
        exit(1)
