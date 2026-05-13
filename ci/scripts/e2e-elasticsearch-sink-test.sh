#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export PATH="$(pwd)/e2e_test/commands:${PATH}"
export ELASTICSEARCH_USER="elastic"
export ELASTICSEARCH_PASSWORD="risingwave"
export RISEDEV_ELASTICSEARCH_URL="http://elasticsearch:9200"
export RISEDEV_ELASTICSEARCH_WITH_OPTIONS_COMMON="connector='elasticsearch',url='${RISEDEV_ELASTICSEARCH_URL}',username='${ELASTICSEARCH_USER}',password='${ELASTICSEARCH_PASSWORD}'"
export OPENSEARCH_USER="admin"
export OPENSEARCH_PASSWORD="Risingwave123!"
export RISEDEV_OPENSEARCH_URL="http://opensearch:9200"
export RISEDEV_OPENSEARCH_WITH_OPTIONS_COMMON="connector='opensearch',url='${RISEDEV_OPENSEARCH_URL}',username='${OPENSEARCH_USER}',password='${OPENSEARCH_PASSWORD}'"

echo "--- check elasticsearch"
for attempt in $(seq 1 60); do
    if curl --fail -sS -u "${ELASTICSEARCH_USER}:${ELASTICSEARCH_PASSWORD}" "${RISEDEV_ELASTICSEARCH_URL}" >/dev/null; then
        break
    fi

    if [[ "$attempt" -eq 60 ]]; then
        echo "Elasticsearch is not ready after 60 attempts."
        exit 1
    fi

    sleep 1
done

echo "--- testing elasticsearch sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/elasticsearch_sink.slt'

echo "--- check opensearch"
opensearch GET /

echo "--- testing opensearch sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/opensearch_sink.slt'
