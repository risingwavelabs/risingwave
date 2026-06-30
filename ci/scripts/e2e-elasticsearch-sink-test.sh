#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export PATH="$(pwd)/e2e_test/commands:${PATH}"
export ELASTICSEARCH_USER="elastic"
export ELASTICSEARCH_PASSWORD="risingwave"
export RISEDEV_ELASTICSEARCH_URL="http://elasticsearch:9200"
export RISEDEV_ELASTICSEARCH_WITH_OPTIONS_COMMON="connector='elasticsearch',url='${RISEDEV_ELASTICSEARCH_URL}',username='${ELASTICSEARCH_USER}',password='${ELASTICSEARCH_PASSWORD}'"

ES_HEALTH_RESPONSE=$(mktemp)
trap 'rm -f "$ES_HEALTH_RESPONSE"' EXIT

echo "--- wait for elasticsearch"
curl -fsS \
  --connect-timeout 2 \
  --max-time 5 \
  --retry 60 \
  --retry-delay 2 \
  --retry-connrefused \
  -u elastic:risingwave \
  "http://elasticsearch:9200/_cluster/health?wait_for_status=yellow&timeout=1s" \
  > "$ES_HEALTH_RESPONSE" || {
    echo "Elasticsearch did not become ready in time"
    curl -fsS -u elastic:risingwave http://elasticsearch:9200/ || true
    exit 1
  }
echo "Elasticsearch is ready:"
cat "$ES_HEALTH_RESPONSE"
echo

echo "--- testing elasticsearch sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/elasticsearch_sink.slt'

sleep 5

echo "--- checking elasticsearch sink result"
curl -fsS -XGET -u elastic:risingwave "http://elasticsearch:9200/test/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' > ./e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
curl -fsS -XGET -u elastic:risingwave "http://elasticsearch:9200/test1/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' > ./e2e_test/sink/elasticsearch/elasticsearch_with_pk_sink.tmp.result
curl -fsS -XGET -u elastic:risingwave "http://elasticsearch:9200/test_route/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' > ./e2e_test/sink/elasticsearch/elasticsearch_with_route.tmp.result
python3 e2e_test/sink/elasticsearch/elasticsearch.py e2e_test/sink/elasticsearch/elasticsearch_sink.result e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
python3 e2e_test/sink/elasticsearch/elasticsearch.py e2e_test/sink/elasticsearch/elasticsearch_with_pk_sink.result e2e_test/sink/elasticsearch/elasticsearch_with_pk_sink.tmp.result
python3 e2e_test/sink/elasticsearch/elasticsearch.py e2e_test/sink/elasticsearch/elasticsearch_with_route.result e2e_test/sink/elasticsearch/elasticsearch_with_route.tmp.result
if [ $? -ne 0 ]; then
  echo "The output is not as expected."
  exit 1
fi
