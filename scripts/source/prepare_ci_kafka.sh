#!/bin/bash

# Exits as soon as any line fails.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1

KAFKA_BIN="$SCRIPT_PATH/../../.risingwave/bin/kafka/bin"

echo "Create topics"
for filename in "$SCRIPT_PATH"/test_data/*; do
    ([ -e "$filename" ]
    base=$(basename "$filename")
    topic="${base%%.*}"
    partition="${base##*.}"

    # always ok
    echo "Drop topic $topic"
    "$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$topic" --delete || true

    echo "Recreate topic $topic with partition $partition"
    "$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$topic" --create --partitions "$partition") &
done
wait

echo "Fulfill kafka topics"
for filename in "$SCRIPT_PATH"/test_data/*; do
    ([ -e "$filename" ]
    base=$(basename "$filename")
    topic="${base%%.*}"

    echo "Fulfill kafka topic $topic with data from $base"
    # binary data, one message a file, filename/topic ends with "bin"
    if [[ "$topic" = *bin ]]; then
        kafkacat -P -b 127.0.0.1:29092 -t "$topic" "$filename"
    else
        cat "$filename" | kafkacat -P -b 127.0.0.1:29092 -t "$topic"
    fi
    ) &
done
wait
