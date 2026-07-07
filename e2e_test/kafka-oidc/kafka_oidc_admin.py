#!/usr/bin/env python3

import os
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def oidc_config(role: str) -> dict[str, str]:
    role_prefix = role.upper()
    return {
        "bootstrap.servers": os.environ["KAFKA_OIDC_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "OAUTHBEARER",
        "sasl.oauthbearer.method": "oidc",
        "sasl.oauthbearer.client.id": os.environ[
            f"KAFKA_OIDC_{role_prefix}_CLIENT_ID"
        ],
        "sasl.oauthbearer.client.secret": os.environ[
            f"KAFKA_OIDC_{role_prefix}_CLIENT_SECRET"
        ],
        "sasl.oauthbearer.token.endpoint.url": os.environ[
            "KAFKA_OIDC_TOKEN_ENDPOINT_URL"
        ],
    }


def admin_client() -> AdminClient:
    return AdminClient(oidc_config("consumer"))


def ignore_unknown_topic_error(exc: Exception) -> None:
    if isinstance(exc, KafkaException):
        error = exc.args[0]
        if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            return
    raise exc


def wait_until_topic_absent(client: AdminClient, topic: str) -> None:
    deadline = time.time() + 30
    while time.time() < deadline:
        metadata = client.list_topics(timeout=10)
        if topic not in metadata.topics:
            return
        time.sleep(1)
    raise TimeoutError(f"topic still exists after delete: {topic}")


def delete_topic(client: AdminClient, topic: str) -> None:
    futures = client.delete_topics([topic], operation_timeout=10)
    try:
        futures[topic].result(timeout=20)
    except Exception as exc:
        ignore_unknown_topic_error(exc)
        return
    wait_until_topic_absent(client, topic)


def recreate_topic(client: AdminClient, topic: str, partitions: int) -> None:
    delete_topic(client, topic)
    futures = client.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)],
        operation_timeout=10,
    )
    futures[topic].result(timeout=20)


def produce_lines(topic: str) -> None:
    producer = Producer(oidc_config("producer"))
    errors = []

    def delivery_report(error, message) -> None:
        if error is not None:
            errors.append(f"{message.topic()}: {error}")

    for line in sys.stdin:
        line = line.rstrip("\n")
        if line:
            producer.produce(topic, line.encode("utf-8"), callback=delivery_report)
            producer.poll(0)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(
            f"failed to produce all records: remaining={remaining}, errors={errors}"
        )


def consume_values(topic: str, count: int, timeout_seconds: int) -> None:
    consumer = Consumer(
        {
            **oidc_config("consumer"),
            "group.id": f"kafka-oidc-e2e-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    deadline = time.time() + timeout_seconds
    values: list[str] = []

    try:
        consumer.subscribe([topic])
        while len(values) < count and time.time() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            values.append(message.value().decode("utf-8"))
    finally:
        consumer.close()

    if len(values) != count:
        raise TimeoutError(f"expected {count} messages from {topic}, got {len(values)}")

    for value in sorted(values):
        print(value)


def fetch_metadata() -> None:
    admin_client().list_topics(timeout=10)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(
            "usage: kafka_oidc_admin.py metadata|recreate-topic|delete-topic|produce-lines|consume-values ..."
        )

    command = sys.argv[1]

    if command == "metadata":
        fetch_metadata()
        return

    if len(sys.argv) < 3:
        raise SystemExit(f"{command} requires a topic argument")

    topic = sys.argv[2]

    if command == "recreate-topic":
        partitions = int(sys.argv[3]) if len(sys.argv) > 3 else 1
        recreate_topic(admin_client(), topic, partitions)
    elif command == "delete-topic":
        delete_topic(admin_client(), topic)
    elif command == "produce-lines":
        produce_lines(topic)
    elif command == "consume-values":
        if len(sys.argv) < 4:
            raise SystemExit("consume-values requires a message count")
        timeout_seconds = int(sys.argv[4]) if len(sys.argv) > 4 else 30
        consume_values(topic, int(sys.argv[3]), timeout_seconds)
    else:
        raise SystemExit(f"unknown command: {command}")


if __name__ == "__main__":
    main()
