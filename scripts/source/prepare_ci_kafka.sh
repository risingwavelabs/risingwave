#!/usr/bin/env bash

# Exits as soon as any line fails.
set -e

KCAT_BIN="kcat"
# kcat bin name on linux is "kafkacat"
if [ "$(uname)" == "Linux" ]
then
    KCAT_BIN="kafkacat"
fi

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1

KAFKA_BIN="$SCRIPT_PATH/../../.risingwave/bin/kafka/bin"

echo "$SCRIPT_PATH"

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
    "$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$topic" --delete || true

    echo "Recreate topic $topic with partition $partition"
    "$KAFKA_BIN"/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic "$topic" --create --partitions "$partition") &
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
        ${KCAT_BIN} -P -b 127.0.0.1:29092 -K ^ -t "$topic" "$filename"
        ${KCAT_BIN} -C -b 127.0.0.1:29092 -K ";" -t "$topic" -e
    else
        cat "$filename" | ${KCAT_BIN} -P -b 127.0.0.1:29092 -t "$topic"
    fi
    ) &
done
wait
