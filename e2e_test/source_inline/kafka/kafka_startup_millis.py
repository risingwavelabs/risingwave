#!/usr/bin/env python3

import argparse
import json
import time
import sys
import os
from confluent_kafka import Producer
import psycopg2


def produce_records(producer, topic, records):
    """Produce records to Kafka topic"""
    for record in records:
        producer.produce(topic, value=json.dumps(record))
    producer.flush()
    print(f"Produced {len(records)} records to topic {topic}")


def main():
    parser = argparse.ArgumentParser(description='Test Kafka startup timestamp functionality')
    parser.add_argument('--db-name', required=True, help='RisingWave database name')
    parser.add_argument('--topic', required=True, help='Kafka topic name')

    args = parser.parse_args()

    kafka_broker = os.environ.get("RISEDEV_KAFKA_BOOTSTRAP_SERVERS")
    # Initialize Kafka producer
    try:
        producer = Producer({
            'bootstrap.servers': kafka_broker
        })
        print(f"Connected to Kafka broker: {kafka_broker}")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

    try:
        # Step 1: Produce some initial records to the topic
        initial_records = [
            {"id": i}
            for i in range(1, 6)
        ]
        produce_records(producer, args.topic, initial_records)

        # Step 2: Sleep 1 second
        print("Sleeping for 1 second...")
        time.sleep(1)

        # Step 3: Record timestamp as millis as CURRENT_TIMESTAMP
        current_timestamp_millis = int(time.time() * 1000)
        print(f"Recorded CURRENT_TIMESTAMP: {current_timestamp_millis}")

        # Step 4: Produce more data
        additional_records = [
            {"id": i}
            for i in range(6, 9)
        ]
        produce_records(producer, args.topic, additional_records)

        # Connect to RisingWave
        try:
            conn = psycopg2.connect(
                host=os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS"),
                port=os.environ.get("RISEDEV_RW_FRONTEND_PORT"),
                user='root',
                password='',
                database=args.db_name
            )
            cur = conn.cursor()
            print(f"Connected to RisingWave database: {args.db_name}")
        except Exception as e:
            print(f"Failed to connect to RisingWave: {e}")
            sys.exit(1)

        table_name = f"test_table_{int(time.time())}"

        try:
            # Step 5: Create a table in RisingWave, starting load from CURRENT_TIMESTAMP
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                message VARCHAR,
                timestamp BIGINT
            ) WITH (
                connector = 'kafka',
                topic = '{args.topic}',
                properties.bootstrap.server = '{kafka_broker}',
                scan.startup.timestamp_millis = '{current_timestamp_millis}'
            ) FORMAT PLAIN ENCODE JSON;
            """

            cur.execute(create_table_sql)
            conn.commit()
            print(f"Created table {table_name} with startup timestamp {current_timestamp_millis}")

            # Wait a moment for the table to start consuming
            time.sleep(3)

            # Step 6: Select count(*) from the table
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count_result = cur.fetchone()[0]
            print(f"Count from table {table_name}: {count_result}")

            # Also show the actual records for verification
            cur.execute(f"SELECT * FROM {table_name} ORDER BY id")
            records = cur.fetchall()
            print(f"Records in table:")
            for record in records:
                print(f"  {record}")

            # Expected: Should only see records with timestamps >= current_timestamp_millis
            expected_count = len([r for r in additional_records])
            if count_result == expected_count:
                print(f"✅ Test PASSED: Got expected count {expected_count}")
            else:
                print(f"❌ Test FAILED: Expected count {expected_count}, got {count_result}")

        finally:
            # Step 7: Drop the table
            try:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()
                print(f"Dropped table {table_name}")
            except Exception as e:
                print(f"Failed to drop table: {e}")

            cur.close()
            conn.close()

    finally:
        pass


if __name__ == "__main__":
    main()
