#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- check elasticsearch"
curl -u elastic:risingwave http://elasticsearch:9200

echo "--- testing elasticsearch sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/elasticsearch_sink.slt'

sleep 5

echo "--- checking elasticsearch sink result"
curl -XGET -u elastic:risingwave "http://elasticsearch:9200/test/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' > ./e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
python3 e2e_test/sink/elasticsearch/elasticsearch.py e2e_test/sink/elasticsearch/elasticsearch_sink.result e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
if [ $? -ne 0 ]; then
  echo "The output is not as expected."
  exit 1
fi
