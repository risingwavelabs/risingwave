#!/usr/bin/env python3
"""
Get current Kafka topic offsets for all partitions.
This is used to inject forward offsets, skipping some messages.
"""
import argparse
import json
import os
from confluent_kafka import Consumer, TopicPartition


def get_kafka_offsets(broker, topic, skip_last_n=2):
    """
    Get offsets for all partitions.
    Args:
        skip_last_n: Skip this many messages from the end by setting offset to high-watermark - skip_last_n
    """
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'test_offset_reader',
        'auto.offset.reset': 'earliest'
    })

    try:
        # Get partition metadata
        metadata = consumer.list_topics(topic)
        partitions = metadata.topics[topic].partitions

        offsets = {}
        for partition_id in partitions.keys():
            tp = TopicPartition(topic, partition_id)
            low, high = consumer.get_watermark_offsets(tp)
            # Set offset to skip last N messages
            # inject_source_offsets expects "last_seen_offset", so start_offset = offset + 1.
            # We subtract 1 to ensure we start reading from (high - skip_last_n).
            new_offset = high - skip_last_n - 1
            offsets[str(partition_id)] = str(new_offset)

        return offsets
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description='Get Kafka topic offsets')
    parser.add_argument('--topic', required=True, help='Kafka topic name')
    parser.add_argument('--output', help='Output file path (default: stdout)')
    parser.add_argument('--skip-last', type=int, default=2,
                       help='Skip last N messages per partition')
    args = parser.parse_args()

    # Get broker from environment
    broker = os.environ.get('RISEDEV_KAFKA_BOOTSTRAP_SERVERS', 'message_queue:29092')

    # Get offsets
    offsets = get_kafka_offsets(broker, args.topic, args.skip_last)

    # Output as JSON
    offsets_json = json.dumps(offsets)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(offsets_json)
        print(f"Offsets written to {args.output}")
    else:
        print(offsets_json)


if __name__ == "__main__":
    main()
