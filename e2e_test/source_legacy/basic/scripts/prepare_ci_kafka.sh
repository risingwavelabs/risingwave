#!/usr/bin/env bash

# Exits as soon as any line fails.
set -e
export CARGO_MAKE_PRINT_TIME_SUMMARY=false

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
# SCRIPT_PATH is e2e_test/source_legacy/kafka/script/
# cwd is e2e_test/source_legacy/kafka/

echo "$SCRIPT_PATH"
cd "$SCRIPT_PATH"

source ../../../../.risingwave/config/risedev-env

if [ "$1" == "compress" ]; then
  echo "Compress test_data/ into test_data.zip"
  zip_file=test_data.zip
  if [ -f "$zip_file" ]; then
    rm "$zip_file"
  fi
  zip -r "$zip_file" ./test_data/ch_benchmark/*
  exit 0
fi

echo "--- Extract data for Kafka"
mkdir -p ./test_data/ch_benchmark/
unzip -o test_data.zip -d .

echo "path:${SCRIPT_PATH}/test_data/**/*"

echo "Create topics"
kafka_data_files=$(find "$SCRIPT_PATH"/test_data -type f)
for filename in $kafka_data_files; do
    ([ -e "$filename" ]
    base=$(basename "$filename")
    topic="${base%%.*}"
    partition="${base##*.}"

    # always ok
    echo "Drop topic $topic"
    risedev rpk topic delete "$topic" || true

    echo "Recreate topic $topic with partition $partition"
    risedev rpk topic create "$topic" --partitions "$partition") &
done
wait

echo "Fulfill kafka topics"
for filename in $kafka_data_files; do
    ([ -e "$filename" ]
    base=$(basename "$filename")
    topic="${base%%.*}"

    echo "Fulfill kafka topic $topic with data from $base"
    # binary data, one message a file, filename/topic ends with "bin"
    if [[ "$topic" = *bin ]]; then
        kcat -P -b "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" -t "$topic" "$filename"
    elif [[ "$topic" = *avro_json ]]; then
        python3 schema_registry_producer.py "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" "${RISEDEV_SCHEMA_REGISTRY_URL}" "$filename" "topic" "avro"
    elif [[ "$topic" = *json_schema ]]; then
        python3 schema_registry_producer.py "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" "${RISEDEV_SCHEMA_REGISTRY_URL}" "$filename" "topic" "json"
    else
        cat "$filename" | kcat -P -K ^  -b "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" -t "$topic"
    fi
    ) &
done
