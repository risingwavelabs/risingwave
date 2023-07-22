from protobuf import user_pb2
import psycopg2
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from time import sleep
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

TOPIC = "sr_pb_test"
SCHEMA_REGISTRY_CONF = {"url": "http://localhost:8081"}
PRODUCER_CONF = {"bootstrap.servers": "localhost:9092"}
NUM_RECORDS = 100


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
    )


def send_to_kafka():
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)
    serializer = ProtobufSerializer(
        user_pb2.User,
        schema_registry_client,
        {"use.deprecated.format": False},
    )

    producer = Producer(PRODUCER_CONF)
    for i in range(NUM_RECORDS):
        user = get_user(i)

        producer.produce(
            topic=TOPIC,
            partition=0,
            value=serializer(user, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report,
        )
    producer.flush()
    print("Send {} records to kafka\n".format(NUM_RECORDS))


if __name__ == "__main__":
    try:
        send_to_kafka()
    except Exception as e:
        print("Send Protobuf data to schema registry and kafka failed {}", e)
        exit(1)
