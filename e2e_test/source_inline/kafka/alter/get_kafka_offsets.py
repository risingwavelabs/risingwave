#!/usr/bin/env python3
"""
Get Kafka offsets for all partitions for inject-source-offsets tests.
"""
import argparse
import json
import os
from confluent_kafka import Consumer, TopicPartition


def get_kafka_offsets(broker, topic, skip_last_n=None, skip_next_n=None):
    """
    Get offsets for all partitions.
    Args:
        skip_last_n: Re-consume the last N messages by setting offset to high-watermark - N - 1.
        skip_next_n: Skip the next N messages by setting offset to high-watermark + N - 1.
    """
    if skip_last_n is not None and skip_next_n is not None:
        raise ValueError("Only one of skip_last_n or skip_next_n can be set")
    if skip_last_n is None and skip_next_n is None:
        skip_last_n = 2

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

            if skip_last_n is not None:
                # inject-source-offsets accepts "last consumed offset".
                # To re-consume the last N messages, the next consumed offset must be (high - N).
                # Therefore we inject (high - N - 1).
                new_offset = high - skip_last_n - 1
            else:
                # To skip the next N messages, the next consumed offset must be (high + N).
                # Therefore we inject (high + N - 1).
                new_offset = high + skip_next_n - 1

            offsets[str(partition_id)] = str(new_offset)

        return offsets
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description='Get Kafka topic offsets')
    parser.add_argument('--topic', required=True, help='Kafka topic name')
    parser.add_argument('--output', help='Output file path (default: stdout)')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--skip-last',
        type=int,
        help='Re-consume the last N messages per partition',
    )
    mode.add_argument(
        '--skip-next',
        type=int,
        help='Skip the next N messages per partition',
    )
    args = parser.parse_args()

    # Get broker from environment
    broker = os.environ.get('RISEDEV_KAFKA_BOOTSTRAP_SERVERS', 'message_queue:29092')

    # Get offsets
    offsets = get_kafka_offsets(broker, args.topic, args.skip_last, args.skip_next)

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
