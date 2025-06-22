#!/usr/bin/env python3

import argparse
import sys
import os
import requests
import pulsar
from pulsar import ConsumerType, InitialPosition


class PulsarCat:
    def __init__(self, broker_url: str, admin_url: str):
        self.broker_url = broker_url
        self.admin_url = admin_url
        self.client = None

    def _normalize_topic(self, topic: str, default_persistent: bool = True) -> str:
        """Normalize topic name by adding scheme and default namespace if missing.
        
        Args:
            topic: Topic name to normalize
            default_persistent: Whether to use persistent:// as default scheme (True) or non-persistent:// (False)
        
        Returns:
            Normalized topic name with full scheme
        """
        if topic.startswith(('persistent://', 'non-persistent://')):
            return topic
        
        scheme = 'persistent://' if default_persistent else 'non-persistent://'
        return f'{scheme}public/default/{topic}'

    def _normalize_topic_with_persistence(self, topic: str, non_persistent: bool = False) -> str:
        """Normalize topic name and handle persistence type conversion.
        
        Args:
            topic: Topic name to normalize
            non_persistent: Whether the topic should be non-persistent
        
        Returns:
            Normalized topic name with appropriate persistence scheme
        """
        # Determine topic type
        if non_persistent:
            if topic.startswith('persistent://'):
                return topic.replace('persistent://', 'non-persistent://', 1)
            elif not topic.startswith('non-persistent://'):
                return f'non-persistent://public/default/{topic}'
            else:
                return topic
        else:
            if topic.startswith('non-persistent://'):
                return topic.replace('non-persistent://', 'persistent://', 1)
            elif not topic.startswith('persistent://'):
                return f'persistent://public/default/{topic}'
            else:
                return topic

    def get_client(self):
        if self.client is None:
            self.client = pulsar.Client(self.broker_url)
        return self.client

    def close(self):
        if self.client:
            self.client.close()

    def create_topic(self, topic: str, partitions: int = 0, non_persistent: bool = False):
        """Create a topic (persistent/non-persistent, partitioned or not)"""

        topic = self._normalize_topic_with_persistence(topic, non_persistent)
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

    def drop_topic(self, topic: str, force: bool = False):
        """Drop/delete a topic"""

        topic = self._normalize_topic(topic)
        print(f"Dropping topic: {topic}")

        # Try to delete as non-partitioned topic first
        url = f"{self.admin_url}/admin/v2/{topic.replace('://', '/')}"
        params = {}
        if force:
            params['force'] = 'true'
        response = requests.delete(url, params=params)

        # If we get a 409 error saying it's a partitioned topic, try deleting as partitioned
        if response.status_code == 409:
            try:
                error_info = response.json()
                if "partitioned topic" in error_info.get("reason", "").lower():
                    print("Detected partitioned topic, trying partitioned topic deletion...")

                    # Delete as partitioned topic
                    partitioned_url = f"{self.admin_url}/admin/v2/{topic.replace('://', '/')}/partitions"
                    partitioned_params = {}
                    if force:
                        partitioned_params['force'] = 'true'
                    response = requests.delete(partitioned_url, params=partitioned_params)

                    if response.status_code in [200, 204]:
                        print(f"Partitioned topic {topic} dropped successfully")
                        print("All partitions have been deleted")
                        return
            except:
                # If we can't parse the error, fall through to general error handling
                pass

        # Handle the response
        if response.status_code in [200, 204]:
            print(f"Topic {topic} dropped successfully")
        elif response.status_code == 404:
            print(f"Topic {topic} not found")
        elif response.status_code == 412:
            print(f"Failed to drop topic: Topic has active subscriptions")
            print("Use --force flag to force deletion or delete subscriptions first")
            sys.exit(1)
        elif response.status_code == 409:
            # Handle other 409 errors that aren't partitioned topic related
            try:
                error_info = response.json()
                reason = error_info.get("reason", "Unknown conflict")
                print(f"Failed to drop topic: {reason}")
            except:
                print(f"Failed to drop topic: 409 - Conflict")
            sys.exit(1)
        else:
            print(f"Failed to drop topic: {response.status_code} - {response.text}")
            sys.exit(1)

    def produce(self, topic: str):
        """Produce messages from stdin, each line with UTF-8 encoding"""
        topic = self._normalize_topic(topic)
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
        topic = self._normalize_topic(topic)
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
        topic = self._normalize_topic(topic)
        print(f"Checking unacknowledged messages for:")
        print(f"  Topic: {topic}")
        print(f"  Subscription: {subscription}")

        # Use admin API to get subscription stats
        encoded_topic = topic.replace('://', '/').replace('/', '%2F')
        url = f"{self.admin_url}/admin/v2/{encoded_topic}/subscriptions/{subscription}"

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
    parser = argparse.ArgumentParser(description='Pulsar Util - Command-line tool for Apache Pulsar')
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

    # Drop topic command
    drop_parser = subparsers.add_parser('drop-topic', help='Drop/delete a topic')
    drop_parser.add_argument('--topic', '-t', required=True, help='Topic name')
    drop_parser.add_argument('--force', '-f', action='store_true', help='Force deletion even if topic has active subscriptions')

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
        elif args.command == 'drop-topic':
            pulsar_cat.drop_topic(args.topic, args.force)
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