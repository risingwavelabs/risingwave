from protobuf import user_pb2
import sys
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
    )


def send_to_kafka(producer_conf, schema_registry_conf, topic, num_records):
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    serializer = ProtobufSerializer(
        user_pb2.User,
        schema_registry_client,
        {"use.deprecated.format": False},
    )

    producer = Producer(producer_conf)
    for i in range(num_records):
        user = get_user(i)

        producer.produce(
            topic=topic,
            partition=0,
            value=serializer(user, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=delivery_report,
        )
    producer.flush()
    print("Send {} records to kafka\n".format(num_records))


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("pb.py <brokerlist> <schema-registry-url> <topic> <num-records>")
        exit(1)

    broker_list = sys.argv[1]
    schema_registry_url = sys.argv[2]
    topic = sys.argv[3]
    num_records = int(sys.argv[4])

    schema_registry_conf = {"url": schema_registry_url}
    producer_conf = {"bootstrap.servers": broker_list}

    try:
        send_to_kafka(producer_conf, schema_registry_conf, topic, num_records)
    except Exception as e:
        print("Send Protobuf data to schema registry and kafka failed {}", e)
        exit(1)
