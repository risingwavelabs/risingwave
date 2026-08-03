#!/usr/bin/env python3
"""
Produce transactional Kafka messages for e2e tests.

Usage:
  python kafka_txn_produce.py --topic TOPIC [--partitions 0,1,2] \
    [--messages-per-partition 2] [--num-transactions 1]

Each message is JSON: {"k": "<partition>-<txn>-<seq>", "v": <global_seq>}
The transactional commit marker after each transaction is the key element:
under read_committed isolation, it occupies an offset but is never delivered
to consumers.
"""

import argparse
import json
import os
import time

from confluent_kafka import Producer


def main():
    parser = argparse.ArgumentParser(description="Produce transactional Kafka messages")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partitions", default="0", help="Comma-separated partition IDs")
    parser.add_argument("--messages-per-partition", type=int, default=1)
    parser.add_argument("--num-transactions", type=int, default=1)
    args = parser.parse_args()

    broker = os.environ.get("RISEDEV_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    partitions = [int(p) for p in args.partitions.split(",")]

    producer = Producer({
        "bootstrap.servers": broker,
        "transactional.id": f"rw-txn-test-{args.topic}-{int(time.time() * 1000)}",
        "transaction.timeout.ms": "60000",
    })
    producer.init_transactions()

    global_seq = 0
    for txn_idx in range(args.num_transactions):
        producer.begin_transaction()
        for p in partitions:
            for seq in range(args.messages_per_partition):
                key = f"{p}-{txn_idx}-{seq}"
                value = json.dumps({"k": key, "v": global_seq})
                producer.produce(args.topic, key=key.encode(), value=value.encode(), partition=p)
                global_seq += 1
        producer.flush()
        producer.commit_transaction()

    print(f"Produced {args.num_transactions} txn(s), {global_seq} messages "
          f"to {args.topic} partitions={partitions}")


if __name__ == "__main__":
    main()
