#!/usr/bin/env python3

import argparse
import os
import threading
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition


def risingwave_conn(db_name: str):
    return psycopg2.connect(
        host=os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS", "localhost"),
        port=os.environ.get("RISEDEV_RW_FRONTEND_PORT", "4566"),
        user="root",
        password="",
        database=db_name,
        options="-c idle_in_transaction_session_timeout=0",
    )


def produce_transactional_message(broker: str, topic: str):
    producer = Producer(
        {
            "bootstrap.servers": broker,
            "transactional.id": f"rw-issue-24829-{int(time.time() * 1000)}",
        }
    )
    producer.init_transactions()
    producer.begin_transaction()
    producer.produce(topic, key=b"k1", value=b"v1", partition=0)
    producer.flush()
    producer.commit_transaction()


def verify_control_record_gap(broker: str, topic: str):
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"rw-issue-24829-verify-{int(time.time() * 1000)}",
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    tp = TopicPartition(topic, 0)
    low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
    consumer.assign([TopicPartition(topic, 0, low)])

    delivered_offsets = []
    deadline = time.time() + 15
    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            raise RuntimeError(f"kafka consume error: {msg.error()}")
        delivered_offsets.append(msg.offset())

    consumer.close()

    if not delivered_offsets:
        raise AssertionError("expected at least one committed data record in transactional topic")
    if high - 1 <= max(delivered_offsets):
        raise AssertionError(
            f"expect control record gap, got high={high}, delivered={delivered_offsets}"
        )
    # This test creates a fresh topic and writes one committed transactional message.
    # Under read_committed:
    # - exactly one data record should be visible
    # - the final offset (`high - 1`) should remain invisible as the COMMIT control record
    #
    # Note: Kafka may place an extra transactional control record before the data record,
    # so the visible data offset is not guaranteed to equal `low`.
    if len(delivered_offsets) != 1:
        raise AssertionError(
            f"expected exactly one visible data record, got {delivered_offsets}"
        )
    data_offset = delivered_offsets[0]
    if high != data_offset + 2:
        raise AssertionError(
            f"expected last offset to be the COMMIT control record (data_offset={data_offset}, high={high})"
        )


def create_mv_and_assert_no_hang(broker: str, db_name: str, topic: str, timeout_secs: int):
    conn = risingwave_conn(db_name)
    conn.autocommit = True
    cur = conn.cursor()

    source_name = f"txn_src_24829_{int(time.time() * 1000)}"
    mv_name = f"txn_mv_24829_{int(time.time() * 1000)}"

    cancel_guard = threading.Event()

    def cancel_if_timeout():
        if not cancel_guard.wait(timeout_secs):
            try:
                conn.cancel()
            except Exception:
                pass

    cancel_thread = threading.Thread(target=cancel_if_timeout, daemon=True)
    cancel_thread.start()

    try:
        cur.execute("SET BACKGROUND_DDL = false")
        cur.execute(
            f"""
            CREATE SOURCE {source_name} (payload bytea)
            WITH (
                connector = 'kafka',
                topic = '{topic}',
                properties.bootstrap.server = '{broker}',
                scan.startup.mode = 'earliest'
            ) FORMAT PLAIN ENCODE BYTES;
            """
        )

        try:
            cur.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS SELECT * FROM {source_name}")
        except psycopg2.errors.QueryCanceled as err:
            raise AssertionError(
                f"create materialized view timed out after {timeout_secs}s: {err}"
            ) from err
        finally:
            cancel_guard.set()

        cur.execute(f"SELECT COUNT(*) FROM {mv_name}")
        row_count = cur.fetchone()[0]
        if row_count != 1:
            raise AssertionError(f"expected 1 row in {mv_name}, got {row_count}")
    finally:
        cancel_guard.set()
        try:
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}")
        except Exception:
            pass
        try:
            cur.execute(f"DROP SOURCE IF EXISTS {source_name}")
        except Exception:
            pass
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Regression test for issue #24829 (Kafka control record backfill hang)"
    )
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--timeout-secs", type=int, default=30)
    args = parser.parse_args()

    broker = os.environ.get("RISEDEV_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    produce_transactional_message(broker, args.topic)
    verify_control_record_gap(broker, args.topic)
    create_mv_and_assert_no_hang(broker, args.db_name, args.topic, args.timeout_secs)

    print("issue #24829 regression test passed")


if __name__ == "__main__":
    main()
