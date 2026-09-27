import asyncio
import base64
import time
import json
import os
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig
from nats.js.errors import NotFoundError
import psycopg2

NATS_SERVER = os.environ.get("NATS_SERVER_URL", os.environ.get("RISEDEV_NATS_SERVER_URL", "nats://nats-server:4222"))

async def create_stream(stream_name: str, subjects: str):
    # Create a NATS client
    nc = NATS()

    try:
        # Connect to the NATS server
        await nc.connect(servers=[NATS_SERVER])

        # split subjects by comma
        subjects = subjects.split(",")
        # Enable JetStream
        js = nc.jetstream()
        stream_config = StreamConfig(
            name=stream_name,
            subjects=subjects,
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
    result_count = 0
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
            result_count = len(results)
            if result_count == expect_count:
                print(f"Found {expect_count} items in {internal_table_name}")
                return
            print(f"Waiting for {internal_table_name} to have {expect_count} items, got {result_count}. Retry...")
            time.sleep(0.5)
    raise Exception(f"Failed to find {expect_count} items in {internal_table_name}, expected {expect_count}, got {result_count}")


async def ensure_all_ack(stream_name: str, consumer_name: str):
    nc = NATS()
    await nc.connect(servers=[NATS_SERVER])
    js = nc.jetstream()

    # Get consumer info
    consumer = await js.consumer_info(stream_name, consumer_name)
    print(f"Consumer stats for {consumer_name} on stream {stream_name}:")
    print(f"  Delivered: {consumer.delivered.consumer_seq}")
    print(f"  Ack Pending: {consumer.num_pending}")
    print(f"  Redelivered: {consumer.num_redelivered}")
    print(f"  Waiting: {consumer.num_waiting}")

    if consumer.num_pending > 0:
        raise Exception(f"Consumer {consumer_name} on stream {stream_name} has {consumer.num_pending} pending messages")
    await nc.close()

async def auth_operation(command: str):
    """Prepare and inspect JetStream data for authentication.slt.serial."""
    import nkeys

    key = nkeys.from_seed(os.environ["NATS_AUTH_NKEY"].encode())
    options = {
        "signature_cb": lambda nonce: base64.b64encode(key.sign(nonce.encode())),
    }
    if os.environ["NATS_AUTH_MODE"] == "jwt":
        options["user_jwt_cb"] = lambda: os.environ["NATS_AUTH_JWT"].encode()
    else:
        options["nkeys_seed_str"] = os.environ["NATS_AUTH_NKEY"]
    nc = NATS()
    await nc.connect(
        os.environ["NATS_AUTH_URL"], allow_reconnect=False, connect_timeout=5,
        max_reconnect_attempts=1,
        **options,
    )
    try:
        js = nc.jetstream()
        if command in ("auth_setup", "auth_cleanup"):
            for stream in ("rw_auth_source", "rw_auth_sink"):
                try:
                    await js.delete_stream(stream)
                except NotFoundError:
                    pass
        if command == "auth_setup":
            for stream in ("rw_auth_source", "rw_auth_sink"):
                await js.add_stream(name=stream, subjects=[stream], storage="memory")
            for i in range(1, 4):
                await js.publish("rw_auth_source", json.dumps({"i": i}).encode())
        elif command == "auth_check_sink":
            info = await js.stream_info("rw_auth_sink")
            rows = set()
            for sequence in range(info.state.first_seq, info.state.last_seq + 1):
                message = await js.get_msg("rw_auth_sink", seq=sequence)
                rows.add(json.loads(message.data)["i"])
            # The sink is at-least-once; allow duplicate deliveries, not missing values.
            assert rows == {4, 5, 6}, f"Unexpected sink values: {rows}"
    finally:
        await nc.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python operation.py <command> [arguments...]")
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
    elif command in ("auth_setup", "auth_check_sink", "auth_cleanup"):
        if len(sys.argv) != 2:
            print(f"Error: {command} does not accept additional arguments")
            sys.exit(1)
        asyncio.run(auth_operation(command))
    elif command == "validate_state":
        if len(sys.argv) != 4:
            print("Error: Both table name and expected count are required")
            sys.exit(1)
        table_name = sys.argv[2]
        expect_count = int(sys.argv[3])
        validate_state_table_item(table_name, expect_count)
    elif command == "ensure_all_ack":
        if len(sys.argv) != 4:
            print("Error: Both stream name and consumer name are required")
            sys.exit(1)
        stream_name = sys.argv[2]
        consumer_name = sys.argv[3]
        asyncio.run(ensure_all_ack(stream_name, consumer_name))
    else:
        print(f"Unknown command: {command}")
        print("Supported commands: create_stream, produce_stream, validate_state, ensure_all_ack, auth_setup, auth_check_sink, auth_cleanup")
        sys.exit(1)
