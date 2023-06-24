#!/usr/bin/env sh
#!/usr/bin/env bash

# Exits as soon as any line fails.
# set -euo pipefail

echo "--- preparing elasticsearch"
# wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
# apt-get install apt-transport-https
# echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/7.x/apt stable main" | tee /etc/apt/sources.list.d/elastic-7.x.list
# apt-get update
# apt-get install elasticsearch
# /bin/systemctl daemon-reload
# /bin/systemctl enable elasticsearch.service
# systemctl start elasticsearch.service

docker pull docker.elastic.co/elasticsearch/elasticsearch:7.17.10
docker run -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.17.10

echo "--- starting elasticsearch"
# timeout 20 elasticsearch-7.17.10/bin/elasticsearch -E http.port=9200
# check status of elasticsearch
curl http://127.0.0.1:9200

echo "--- testing sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch_sink.slt'


echo "testing sink result"
# curl result is of the form
# {"took":3,"timed_out":false,"_shards": .. }, took varies from query and therefore
# we cut "{"took":3,"timed_out":false,"_shards":" out by awk.
curl curl -XGET "http://127.0.0.1:9200/test/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' | awk -F"_shards\":{" '{print$2}' | awk -F"}}]}}" '{print $1}' > ./e2e_test/sink/elasticsearch_sink.tmp.result
diff ./e2e_test/sink/elasticsearch_sink.result ./e2e_test/sink/elasticsearch_sink.tmp.result
if [ $? -ne 0 ]; then
  echo "The output is not as expected."
  exit 1
fi
