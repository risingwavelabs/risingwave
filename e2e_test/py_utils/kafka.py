from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient


def create_kafka_producer(producer_conf):
    """Create a Kafka producer from the provided configuration."""
    return Producer(producer_conf)


def create_schema_registry_client(schema_registry_conf):
    """Create a schema registry client from the provided configuration."""
    return SchemaRegistryClient(schema_registry_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.value()}: {err}")


def produce_serialized_messages(
    producer,
    topic,
    messages,
    *,
    partition=0,
    on_delivery=delivery_report,
):
    """Produce pre-serialized Kafka messages and flush once at the end."""
    for message in messages:
        produce_kwargs = {
            "topic": topic,
            "partition": partition,
            "on_delivery": on_delivery,
        }

        key = message.get("key")
        value = message.get("value")

        if key is not None:
            produce_kwargs["key"] = key
        if value is not None:
            produce_kwargs["value"] = value

        producer.produce(**produce_kwargs)

    producer.flush()
