#!/usr/bin/env python3

import argparse
import json
import time
import sys
import os
import pulsar
import psycopg2


def produce_records(producer, records):
    """Produce records to Pulsar topic"""
    for record in records:
        producer.send(json.dumps(record).encode('utf-8'))
    print(f"Produced {len(records)} records to topic")


def main():
    parser = argparse.ArgumentParser(description='Test Pulsar startup timestamp functionality')
    parser.add_argument('--db-name', required=True, help='RisingWave database name')
    parser.add_argument('--topic', required=True, help='Pulsar topic name')
    parser.add_argument('--produce-after-table', action='store_true', help='Produce more data after building table')

    args = parser.parse_args()

    pulsar_broker_url = os.environ.get("PULSAR_BROKER_URL", "pulsar://localhost:6650")
    topic_name = f"persistent://public/default/{args.topic}"

    # Initialize Pulsar client and producer
    try:
        client = pulsar.Client(pulsar_broker_url)
        producer = client.create_producer(topic_name)
        print(f"Connected to Pulsar broker: {pulsar_broker_url}")
        print(f"Producing to topic: {topic_name}")
    except Exception as e:
        print(f"Failed to connect to Pulsar: {e}")
        sys.exit(1)

    try:
        # Step 1: Produce some initial records to the topic
        initial_records = [
            {"id": i}
            for i in range(1, 6)
        ]
        produce_records(producer, initial_records)
        producer.flush()

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

        if not args.produce_after_table:
            print("Producing more data before table is created")
            produce_records(producer, additional_records)
            producer.flush()

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
                connector = 'pulsar',
                service.url = '{pulsar_broker_url}',
                topic = '{topic_name}',
                scan.startup.timestamp.millis = '{current_timestamp_millis}'
            ) FORMAT PLAIN ENCODE JSON;
            """

            cur.execute(create_table_sql)
            conn.commit()
            print(f"Created table {table_name} with startup timestamp {current_timestamp_millis}")

            if args.produce_after_table:
                print("Producing more data after table is created")
                produce_records(producer, additional_records)
                producer.flush()

            # Wait a moment for the table to start consuming
            time.sleep(3)

            # Step 6: Select count(*) from the table with retry logic
            expected_count = len([r for r in additional_records])
            max_retries = 3
            retry_delay = 1  # 1 second backoff

            for retry_attempt in range(max_retries + 1):
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count_result = cur.fetchone()[0]
                print(f"Count from table {table_name}: {count_result} (attempt {retry_attempt + 1}/{max_retries + 1})")

                # Also show the actual records for verification
                cur.execute(f"SELECT * FROM {table_name} ORDER BY id")
                records = cur.fetchall()
                print(f"Records in table:")
                for record in records:
                    print(f"  {record}")

                # Check if we got the expected count
                if count_result == expected_count:
                    print(f"✅ Test PASSED: Expected count {expected_count}, got {count_result}")
                    break
                elif retry_attempt < max_retries:
                    print(f"⏳ Retrying in {retry_delay} seconds... (expected {expected_count}, got {count_result})")
                    time.sleep(retry_delay)
                else:
                    # Final attempt failed
                    assert count_result == expected_count, f"❌ Test FAILED: Expected count {expected_count}, got {count_result}"

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
        producer.close()
        client.close()


if __name__ == "__main__":
    main()