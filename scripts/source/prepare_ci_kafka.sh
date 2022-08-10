#!/bin/bash

# Exits as soon as any line fails.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1

KAFKA_BIN="$SCRIPT_PATH/../../.risingwave/bin/kafka/bin"
SINK_TARGET_TOPIC="sink_target"
SINK_DEBEZIUM_TOPIC="sink_debezium"

echo "Create topics"
for filename in "$SCRIPT_PATH"/test_data/*; do
    [ -e "$filename" ] || continue
    base=$(basename "$filename")
    topic="${base%%.*}"
    partition="${base##*.}"

    # always ok
    echo "Drop topic $topic"
    "$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$topic" --delete || true

    echo "Recreate topic $topic with partition $partition"
    "$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$topic" --create --partitions "$partition"
done

# prepare for sink test
"$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$SINK_TARGET_TOPIC" --delete || true
"$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$SINK_TARGET_TOPIC" --create --partitions 1
"$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$SINK_DEBEZIUM_TOPIC" --delete || true
"$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$SINK_DEBEZIUM_TOPIC" --create --partitions 1

echo "Fulfill kafka topics"
for filename in "$SCRIPT_PATH"/test_data/*; do
    [ -e "$filename" ] || continue
    base=$(basename "$filename")
    topic="${base%%.*}"

    echo "Fulfill kafka topic $topic with data from $base"
    "$KAFKA_BIN"/kafka-console-producer.sh --broker-list 127.0.0.1:29092 --topic "$topic" < "$filename"
done
