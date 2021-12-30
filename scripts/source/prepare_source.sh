#!/bin/bash

# Exits as soon as any line fails.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1

wait_server() {
    # https://stackoverflow.com/a/44484835/5242660
    # Licensed by https://creativecommons.org/licenses/by-sa/3.0/
    {
        failed_times=0
        while ! echo -n > /dev/tcp/localhost/"$1"; do
            sleep 0.5
            failed_times=$((failed_times+1))
            if [ $failed_times -gt 30 ]; then
                echo "ERROR: failed to start server $1 [timeout=15s]"
                exit 1
            fi
        done
    } 2>/dev/null
}

echo "Starting single node zookeeper/kafka/mysql/debezium"
docker compose -f "$SCRIPT_PATH"/docker-compose.yml up -d

echo "Waiting for zookeeper"
wait_server 2181

echo "Waiting for kafka broker"
wait_server 29092

echo "Waiting for mysql source"
wait_server 23306

echo "Waiting for debezium"
wait_server 28083

echo "Waiting for cluster"
sleep 10

echo "Create topics"
for filename in "$SCRIPT_PATH"/test_data/* ; do
    [ -e "$filename" ] || continue
    base=$(basename "$filename")
    topic="${base%%.*}"
    partition="${base##*.}"

    # always ok
    echo "Drop topic $topic"
    docker exec -i broker /usr/bin/kafka-topics --bootstrap-server broker:9092 --topic "$topic" --delete || true

    echo "Recreate topic $topic with partition $partition"
    docker exec -i broker /usr/bin/kafka-topics --bootstrap-server broker:9092 --topic "$topic" --create --partitions "$partition"
done


echo "Fulfill kafka topics"
for filename in "$SCRIPT_PATH"/test_data/* ; do
    [ -e "$filename" ] || continue
    base=$(basename "$filename")
    topic="${base%%.*}"

    echo "Fulfill kafka topic $topic with data from $base"
    # Note the -l parameter here, without which -l will treat the entire file as a single message
    docker exec -i kafkacat kafkacat -b broker:9092 -t "$topic" -l -P /streaming/test_data/"$base"
done

echo "Creating sync job for debezium and mysql"
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:28083/connectors/ -d @"$SCRIPT_PATH"/debezium-mysql.json
