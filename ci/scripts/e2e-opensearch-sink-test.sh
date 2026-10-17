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
export SEARCH_SINK_ROUTE_WITH_OPTIONS="${RISEDEV_OPENSEARCH_WITH_OPTIONS_COMMON}"

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

echo "--- testing opensearch sigv4 sink"
MOCK_OPENSEARCH_SIGV4_URL="http://127.0.0.1:19200"
mock_log="$(mktemp)"
python3 ./e2e_test/sink/elasticsearch/mock_opensearch_sigv4.py \
    --port 19200 >"${mock_log}" 2>&1 &
mock_pid="$!"
cleanup_mock() {
    kill "${mock_pid}" >/dev/null 2>&1 || true
    wait "${mock_pid}" >/dev/null 2>&1 || true
    rm -f "${mock_log}"
}
trap cleanup_mock EXIT

for attempt in $(seq 1 30); do
    if curl --fail -sS --connect-timeout 2 --max-time 5 "${MOCK_OPENSEARCH_SIGV4_URL}/__stats" >/dev/null; then
        break
    fi

    if [[ "$attempt" -eq 30 ]]; then
        echo "OpenSearch SigV4 mock is not ready after 30 attempts."
        cat "${mock_log}"
        exit 1
    fi

    sleep 1
done

MOCK_OPENSEARCH_SIGV4_URL="${MOCK_OPENSEARCH_SIGV4_URL}" \
    sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/opensearch_sigv4_sink.slt'
