#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export PATH="$(pwd)/e2e_test/commands:${PATH}"
export ELASTICSEARCH_USER="elastic"
export ELASTICSEARCH_PASSWORD="risingwave"
export RISEDEV_ELASTICSEARCH_URL="http://elasticsearch:9200"
export RISEDEV_ELASTICSEARCH_WITH_OPTIONS_COMMON="connector='elasticsearch',url='${RISEDEV_ELASTICSEARCH_URL}',username='${ELASTICSEARCH_USER}',password='${ELASTICSEARCH_PASSWORD}'"

echo "--- check elasticsearch"
elasticsearch GET /

echo "--- testing elasticsearch sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/elasticsearch_sink.slt'
