#!/usr/bin/env python3

import argparse
import os
import time

from confluent_kafka import Producer


def produce_transactional_message(broker: str, topic: str) -> None:
    producer = Producer(
        {
            "bootstrap.servers": broker,
            "transactional.id": f"rw-issue-24829-{int(time.time() * 1000)}",
        }
    )
    producer.init_transactions()
    producer.begin_transaction()
    producer.produce(topic, key=b"k1", value=b"v1", partition=0)
    producer.flush()
    producer.commit_transaction()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare transactional Kafka data for issue #24829 regression test"
    )
    parser.add_argument("--topic", required=True)
    args = parser.parse_args()

    broker = os.environ.get("RISEDEV_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    produce_transactional_message(broker, args.topic)


if __name__ == "__main__":
    main()
