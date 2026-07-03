#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export PATH="$(pwd)/e2e_test/commands:${PATH}"
export OPENSEARCH_USER="admin"
export OPENSEARCH_PASSWORD="Risingwave123!"
export RISEDEV_OPENSEARCH_URL="http://opensearch:9200"
export RISEDEV_OPENSEARCH_WITH_OPTIONS_COMMON="connector='opensearch',url='${RISEDEV_OPENSEARCH_URL}',username='${OPENSEARCH_USER}',password='${OPENSEARCH_PASSWORD}'"
export SEARCH_SINK_CONNECTOR="opensearch"
export SEARCH_SINK_CONNECTION_TYPE="elasticsearch"
export SEARCH_SINK_URL="${RISEDEV_OPENSEARCH_URL}"
export SEARCH_SINK_USER="${OPENSEARCH_USER}"
export SEARCH_SINK_PASSWORD="${OPENSEARCH_PASSWORD}"
export SEARCH_SINK_WITH_OPTIONS_COMMON="${RISEDEV_OPENSEARCH_WITH_OPTIONS_COMMON}"

echo "--- check opensearch"
for attempt in $(seq 1 60); do
    if curl --fail -sS --connect-timeout 2 --max-time 5 -u "${OPENSEARCH_USER}:${OPENSEARCH_PASSWORD}" "${RISEDEV_OPENSEARCH_URL}" >/dev/null; then
        break
    fi

    if [[ "$attempt" -eq 60 ]]; then
        echo "OpenSearch is not ready after 60 attempts."
        exit 1
    fi

    sleep 1
done

echo "--- testing opensearch sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/elasticsearch_sink.slt'
