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

FILE="./source/alter_data/kafka_alter.$1"
echo "Send data from $FILE"
cat $FILE | ${KCAT_BIN} -P -b message_queue:29092 -t kafka_alter