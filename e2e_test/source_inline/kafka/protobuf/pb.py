import sys
import json
import importlib
from google.protobuf.source_context_pb2 import SourceContext
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.value(), err))


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


def get_user_with_nested_struct(i):
    return user_pb2.User(
        id=i,
        name="User_{}".format(i),
        address="Address_{}".format(i),
        city="City_{}".format(i),
        gender=user_pb2.MALE if i % 2 == 0 else user_pb2.FEMALE,
        sc=SourceContext(file_name="source/context_{:03}.proto".format(i)),
        address_detail=user_pb2.AddressDetail(
            street="Street_{}".format(i),
            city="City_{}".format(i),
            location_detail=user_pb2.LocationDetail(
                latitude=37.7749 + i * 0.01,
                longitude=-122.4194 + i * 0.01,
            ),
        ),
    )


def get_user_with_nested_struct_extended(i):
    return user_pb2.User(
        id=i,
        name="User_{}".format(i),
        address="Address_{}".format(i),
        city="City_{}".format(i),
        gender=user_pb2.MALE if i % 2 == 0 else user_pb2.FEMALE,
        sc=SourceContext(file_name="source/context_{:03}.proto".format(i)),
        address_detail=user_pb2.AddressDetail(
            street="Street_{}".format(i),
            city="City_{}".format(i),
            location_detail=user_pb2.LocationDetail(
                latitude=37.7749 + i * 0.01,
                longitude=-122.4194 + i * 0.01,
                altitude=100.0 + i * 10.0,
            ),
            country="Country_{}".format(i),
        ),
    )


def send_to_kafka(
    producer_conf, schema_registry_conf, topic, num_records, get_message_fn, pb_message_class
):
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    serializer = ProtobufSerializer(
        pb_message_class,
        schema_registry_client,
        {"use.deprecated.format": False, "skip.known.types": True},
    )

    producer = Producer(producer_conf)
    for i in range(num_records):
        message = get_message_fn(i)

        producer.produce(
            topic=topic,
            partition=0,
            key=json.dumps({"id": i}), # RisingWave does not handle key schema, so we use json
            value=serializer(message, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=delivery_report,
        )
    producer.flush()
    print("Send {} records to kafka\n".format(num_records))


if __name__ == "__main__":
    if len(sys.argv) < 5:
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
        "user_with_nested_struct": get_user_with_nested_struct,
        "user_with_nested_struct_extended": get_user_with_nested_struct_extended,
    }

    assert (
        pb_message in all_pb_messages
    ), f"pb_message must be one of {list(all_pb_messages.keys())}"

    schema_registry_conf = {"url": schema_registry_url}
    producer_conf = {"bootstrap.servers": broker_list}

    # Determine the protobuf message class based on the message name
    pb_message_classes = {
        "user": user_pb2.User,
        "user_with_more_fields": user_pb2.User,
        "user_with_nested_struct": user_pb2.User,
        "user_with_nested_struct_extended": user_pb2.User,
    }

    try:
        send_to_kafka(
            producer_conf,
            schema_registry_conf,
            topic,
            num_records,
            all_pb_messages[pb_message],
            pb_message_classes[pb_message],
        )
    except Exception as e:
        print("Send Protobuf data to schema registry and kafka failed {}", e)
        exit(1)
