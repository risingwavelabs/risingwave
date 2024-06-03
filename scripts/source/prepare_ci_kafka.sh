#!/usr/bin/env bash

# Exits as soon as any line fails.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1
# cwd is /scripts

echo "$SCRIPT_PATH"

source ../.risingwave/config/risedev-env

if [ "$1" == "compress" ]; then
  echo "Compress test_data/ into test_data.zip"
  cd ./source
  zip_file=test_data.zip
  if [ -f "$zip_file" ]; then
    rm "$zip_file"
  fi
  zip -r "$zip_file" ./test_data/ch_benchmark/*
  exit 0
fi

echo "--- Extract data for Kafka"
cd ./source/
mkdir -p ./test_data/ch_benchmark/
unzip -o test_data.zip -d .
cd ..

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
        python3 source/schema_registry_producer.py "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" "${RISEDEV_SCHEMA_REGISTRY_URL}" "$filename" "topic" "avro"
    elif [[ "$topic" = *json_schema ]]; then
        python3 source/schema_registry_producer.py "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" "${RISEDEV_SCHEMA_REGISTRY_URL}" "$filename" "topic" "json"
    else
        cat "$filename" | kcat -P -K ^  -b "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" -t "$topic"
    fi
    ) &
done

# test additional columns: produce messages with headers
ADDI_COLUMN_TOPIC="kafka_additional_columns"
for i in {0..100}; do echo "key$i:{\"a\": $i}" | kcat -P -b "${RISEDEV_KAFKA_BOOTSTRAP_SERVERS}" -t ${ADDI_COLUMN_TOPIC} -K : -H "header1=v1" -H "header2=v2"; done
