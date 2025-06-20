#!/usr/bin/env python3
"""
Pulsar Cat - A command-line tool for Apache Pulsar operations
Similar to kafka-cat but for Pulsar

Usage:
    python pulsar_cat.py create-topic --topic <topic> [--partitions <n>] [--non-persistent]
    python pulsar_cat.py produce --topic <topic> [--broker <url>]
    python pulsar_cat.py consume --topic <topic> --subscription <sub> [--broker <url>] [--position <pos>]
    python pulsar_cat.py unacked --topic <topic> --subscription <sub> [--broker <url>]
"""

import argparse
import sys
import os
import json
import time
from typing import Optional
import requests
import pulsar
from pulsar import ConsumerType, InitialPosition


class PulsarCat:
    def __init__(self, broker_url: str, admin_url: str):
        self.broker_url = broker_url
        self.admin_url = admin_url
        self.client = None

    def get_client(self):
        if self.client is None:
            self.client = pulsar.Client(self.broker_url)
        return self.client

    def close(self):
        if self.client:
            self.client.close()

    def create_topic(self, topic: str, partitions: int = 0, non_persistent: bool = False):
        """Create a topic (persistent/non-persistent, partitioned or not)"""
        
        # Determine topic type
        if non_persistent:
            if topic.startswith('persistent://'):
                topic = topic.replace('persistent://', 'non-persistent://', 1)
            elif not topic.startswith('non-persistent://'):
                topic = f'non-persistent://public/default/{topic}'
        else:
            if topic.startswith('non-persistent://'):
                topic = topic.replace('non-persistent://', 'persistent://', 1)
            elif not topic.startswith('persistent://'):
                topic = f'persistent://public/default/{topic}'

        print(f"Creating topic: {topic}")
        
        if partitions > 0:
            # Create partitioned topic
            url = f"{self.admin_url}/admin/v2/{topic.replace('://', '/')}/partitions"
            response = requests.put(url, json=partitions)
        else:
            # Create non-partitioned topic  
            url = f"{self.admin_url}/admin/v2/{topic.replace('://', '/')}"
            response = requests.put(url)

        if response.status_code in [200, 204, 409]:  # 409 means already exists
            if response.status_code == 409:
                print(f"Topic {topic} already exists")
            else:
                print(f"Topic {topic} created successfully")
                if partitions > 0:
                    print(f"Partitions: {partitions}")
                print(f"Type: {'non-persistent' if non_persistent else 'persistent'}")
        else:
            print(f"Failed to create topic: {response.status_code} - {response.text}")
            sys.exit(1)

    def produce(self, topic: str):
        """Produce messages from stdin, each line with UTF-8 encoding"""
        if not topic.startswith(('persistent://', 'non-persistent://')):
            topic = f'persistent://public/default/{topic}'
            
        print(f"Producing to topic: {topic}")
        print("Type messages (Ctrl+C to stop):")
        
        client = self.get_client()
        producer = client.create_producer(topic)
        
        try:
            line_count = 0
            for line in sys.stdin:
                line = line.rstrip('\n\r')
                if line:  # Only send non-empty lines
                    producer.send(line.encode('utf-8'))
                    line_count += 1
                    print(f"Sent message {line_count}: {line}")
        except KeyboardInterrupt:
            print(f"\nProduced {line_count} messages")
        finally:
            producer.close()

    def consume(self, topic: str, subscription: str, position: str = 'latest', exit_on_end: bool = False):
        """Consume messages from a topic"""
        if not topic.startswith(('persistent://', 'non-persistent://')):
            topic = f'persistent://public/default/{topic}'
            
        print(f"Consuming from topic: {topic}")
        print(f"Subscription: {subscription}")
        print(f"Position: {position}")
        if exit_on_end:
            print("Mode: Exit when no more messages available")
        else:
            print("Mode: Keep waiting for new messages (Ctrl+C to stop)")
        
        client = self.get_client()
        
        # Set initial position
        initial_pos = InitialPosition.Latest
        if position.lower() == 'earliest':
            initial_pos = InitialPosition.Earliest
        
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_type=ConsumerType.Shared,
            initial_position=initial_pos
        )
        
        try:
            message_count = 0
            consecutive_timeouts = 0
            max_consecutive_timeouts = 3  # Exit after 3 consecutive timeouts in exit_on_end mode
            
            while True:
                try:
                    # Use shorter timeout for exit_on_end mode to be more responsive
                    timeout_ms = 1000 if exit_on_end else 5000
                    msg = consumer.receive(timeout_millis=timeout_ms)
                    message_count += 1
                    consecutive_timeouts = 0  # Reset timeout counter on successful receive
                    
                    # Decode message
                    try:
                        content = msg.data().decode('utf-8')
                    except UnicodeDecodeError:
                        content = f"<binary data: {len(msg.data())} bytes>"
                    
                    # Print message info
                    print(f"Message {message_count}:")
                    print(f"  Topic: {msg.topic_name()}")
                    print(f"  Message ID: {msg.message_id()}")
                    print(f"  Publish Time: {msg.publish_timestamp()}")
                    print(f"  Properties: {msg.properties()}")
                    print(f"  Content: {content}")
                    print("---")
                    
                    # Acknowledge the message
                    consumer.acknowledge(msg)
                    
                except Exception as e:
                    # More robust timeout detection
                    error_str = str(e).lower()
                    is_timeout = any(keyword in error_str for keyword in ['timeout', 'timed out', 'no message available'])
                    
                    if is_timeout:
                        consecutive_timeouts += 1
                        if exit_on_end:
                            print(f"Timeout {consecutive_timeouts}/{max_consecutive_timeouts}")
                            if consecutive_timeouts >= max_consecutive_timeouts:
                                print(f"No more messages available after {max_consecutive_timeouts} timeout attempts. Exiting.")
                                break
                        # In normal mode, just continue waiting silently
                        continue
                    else:
                        print(f"Error receiving message: {e}")
                        consecutive_timeouts = 0
                        continue
                    
        except KeyboardInterrupt:
            print(f"\nConsumed {message_count} messages")
        finally:
            consumer.close()
            if exit_on_end:
                print(f"Total messages consumed: {message_count}")

    def check_unacked(self, topic: str, subscription: str):
        """Check unacknowledged messages for a subscription"""
        if not topic.startswith(('persistent://', 'non-persistent://')):
            topic = f'persistent://public/default/{topic}'
            
        print(f"Checking unacknowledged messages for:")
        print(f"  Topic: {topic}")
        print(f"  Subscription: {subscription}")
        
        # Use admin API to get subscription stats
        encoded_topic = topic.replace('://', '/').replace('/', '%2F')
        url = f"{self.admin_url}/admin/v2/{topic.replace('://', '/')}/subscriptions/{subscription}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                stats = response.json()
                
                print("\nSubscription Statistics:")
                print(f"  Type: {stats.get('type', 'N/A')}")
                print(f"  Total Messages: {stats.get('msgOutCounter', 0)}")
                print(f"  Messages Rate (per sec): {stats.get('msgOutRate', 0)}")
                print(f"  Bytes Rate (per sec): {stats.get('bytesOutRate', 0)}")
                print(f"  Unacked Messages: {stats.get('unackedMessages', 0)}")
                print(f"  Blocked on Unacked Messages: {stats.get('blockedSubscriptionOnUnackedMsgs', False)}")
                
                consumers = stats.get('consumers', [])
                print(f"  Active Consumers: {len(consumers)}")
                
                for i, consumer in enumerate(consumers):
                    print(f"    Consumer {i + 1}:")
                    print(f"      Name: {consumer.get('consumerName', 'N/A')}")
                    print(f"      Unacked Messages: {consumer.get('unackedMessages', 0)}")
                    print(f"      Blocked: {consumer.get('blockedConsumerOnUnackedMsgs', False)}")
                    
            elif response.status_code == 404:
                print("Subscription not found")
            else:
                print(f"Failed to get subscription stats: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Error checking unacked messages: {e}")


def main():
    parser = argparse.ArgumentParser(description='Pulsar Cat - Command-line tool for Apache Pulsar')
    parser.add_argument('--broker', '-b', 
                       default=os.environ.get('PULSAR_BROKER_URL', 'pulsar://localhost:6650'),
                       help='Pulsar broker URL (default: pulsar://localhost:6650)')
    parser.add_argument('--admin-url', '-a',
                       default=os.environ.get('PULSAR_HTTP_URL', 'http://localhost:8080'),
                       help='Pulsar admin HTTP URL (default: http://localhost:8080)')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Create topic command
    create_parser = subparsers.add_parser('create-topic', help='Create a topic')
    create_parser.add_argument('--topic', '-t', required=True, help='Topic name')
    create_parser.add_argument('--partitions', '-p', type=int, default=0, help='Number of partitions (0 = non-partitioned)')
    create_parser.add_argument('--non-persistent', action='store_true', help='Create non-persistent topic')
    
    # Produce command
    produce_parser = subparsers.add_parser('produce', help='Produce messages from stdin')
    produce_parser.add_argument('--topic', '-t', required=True, help='Topic name')
    
    # Consume command
    consume_parser = subparsers.add_parser('consume', help='Consume messages from a topic')
    consume_parser.add_argument('--topic', '-t', required=True, help='Topic name')
    consume_parser.add_argument('--subscription', '-s', required=True, help='Subscription name')
    consume_parser.add_argument('--position', choices=['earliest', 'latest'], default='latest',
                               help='Starting position (default: latest)')
    consume_parser.add_argument('--exit-on-end', action='store_true',
                               help='Exit when no more messages are available instead of waiting for new ones')
    
    # Unacked command
    unacked_parser = subparsers.add_parser('unacked', help='Check unacknowledged messages')
    unacked_parser.add_argument('--topic', '-t', required=True, help='Topic name')
    unacked_parser.add_argument('--subscription', '-s', required=True, help='Subscription name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Create PulsarCat instance
    pulsar_cat = PulsarCat(args.broker, args.admin_url)
    
    try:
        if args.command == 'create-topic':
            pulsar_cat.create_topic(args.topic, args.partitions, args.non_persistent)
        elif args.command == 'produce':
            pulsar_cat.produce(args.topic)
        elif args.command == 'consume':
            pulsar_cat.consume(args.topic, args.subscription, args.position, args.exit_on_end)
        elif args.command == 'unacked':
            pulsar_cat.check_unacked(args.topic, args.subscription)
    finally:
        pulsar_cat.close()


if __name__ == '__main__':
    main() 