#!/usr/bin/env python3

import sys
import os

import pulsar


def main():
    broker_url = os.environ.get('PULSAR_BROKER_URL', 'pulsar://localhost:6650')
    topic = sys.argv[1]

    client = pulsar.Client(broker_url)
    producer = client.create_producer(topic)
    for l in sys.stdin:
        producer.send(l.encode('utf-8'))
    client.close()


if __name__ == '__main__':
    main()
