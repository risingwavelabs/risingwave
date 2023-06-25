#!/usr/bin/env sh
#!/usr/bin/env bash

# Exits as soon as any line fails.
# set -euo pipefailp

echo "--- check elasticsearch"
curl http://elasticsearch:9200

echo "--- testing sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/elasticsearch_sink.slt'


echo "testing sink result"
# curl result is of the form
# {"took":3,"timed_out":false,"_shards": .. }, took varies from query and therefore
# we cut "{"took":3,"timed_out":false,"_shards":" out by awk.
curl curl -XGET "http://elasticsearch:9200/test/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' | awk -F"_shards\":{" '{print$2}' | awk -F"}}]}}" '{print $1}' > ./e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
diff ./e2e_test/sink/elasticsearch/elasticsearch_sink.result ./e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
if [ $? -ne 0 ]; then
  echo "The output is not as expected."
  exit 1
fi
