import asyncio
import time
import json
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig
import psycopg2

NATS_SERVER = "nats://nats-server:4222"

async def create_stream(stream_name: str, subject: str):
    # Create a NATS client
    nc = NATS()

    try:
        # Connect to the NATS server
        await nc.connect(servers=[NATS_SERVER])

        # Enable JetStream
        js = nc.jetstream()
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[subject],
            retention="limits",  # Retention policy (limits, interest, or workqueue)
            max_msgs=1000,       # Maximum number of messages to retain
            max_bytes=10 * 1024 * 1024,  # Maximum size of messages in bytes
            max_age=0,           # Maximum age of messages (0 means unlimited)
            storage="file",      # Storage type (file or memory)
        )

        # Add the stream
        await js.add_stream(stream_config)
        print(f"Stream '{stream_name}' added successfully with subject '{subject}'.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        await nc.close()
        print("Disconnected from NATS server.")

async def produce_message(stream_name: str, subject: str):
    nc = NATS()
    await nc.connect(servers=[NATS_SERVER])
    js = nc.jetstream()
    for i in range(100):
        payload = {"i": i}
        await js.publish(subject, str.encode(json.dumps(payload)))

    await nc.close()


async def consume_message(_stream_name: str, subject: str):
    nc = NATS()
    await nc.connect(servers=[NATS_SERVER])
    js = nc.jetstream()
    consumer = await js.pull_subscribe(subject)
    for i in range(100):
        msgs = await consumer.fetch(1)
        for msg in msgs:
            print(msg.data)
            await msg.ack()


def validate_state_table_item(table_name: str, expect_count: int):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    # query for the internal table name and make sure it exists
    with conn.cursor() as cursor:
        cursor.execute(f"select name from rw_internal_table_info where job_name = '{table_name}'")
        results = cursor.fetchall()
        assert len(results) == 1, f"Expected exactly one internal table matching {table_name}, found {len(results)}"
        internal_table_name = results[0][0]
        print(f"Found internal table: {internal_table_name}")
        for _ in range(10):
            cursor.execute(f"SELECT * FROM {internal_table_name}")
            results = cursor.fetchall()
            print(f"Get items from state table: {results}")
            if len(results) == expect_count:
                print(f"Found {expect_count} items in {internal_table_name}")
                break
            print(f"Waiting for {internal_table_name} to have {expect_count} items, got {len(results)}. Retry...")
            time.sleep(0.5)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python operation.py <command> <stream_name> <subject> [table_name] [expect_count]")
        sys.exit(1)

    command = sys.argv[1]
    if command in ["create_stream", "produce_stream"]:
        if len(sys.argv) != 4:
            print("Error: Both stream name and subject are required")
            sys.exit(1)
        stream_name = sys.argv[2]
        subject = sys.argv[3]

        if command == "create_stream":
            asyncio.run(create_stream(stream_name, subject))
        elif command == "produce_stream":
            asyncio.run(produce_message(stream_name, subject))
    elif command == "validate_state":
        if len(sys.argv) != 4:
            print("Error: Both table name and expected count are required")
            sys.exit(1)
        table_name = sys.argv[2]
        expect_count = int(sys.argv[3])
        validate_state_table_item(table_name, expect_count)
    else:
        print(f"Unknown command: {command}")
        print("Supported commands: create_stream, produce_stream, validate_state")
        sys.exit(1)
