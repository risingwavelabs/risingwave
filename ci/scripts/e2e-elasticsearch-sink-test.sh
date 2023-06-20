#!/usr/bin/env sh
#!/usr/bin/env bash

# Exits as soon as any line fails.
# set -euo pipefail

echo "--- preparing elasticsearch"
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.17.10-linux-x86_64.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.17.10-linux-x86_64.tar.gz.sha512
shasum -a 512 -c elasticsearch-7.17.10-linux-x86_64.tar.gz.sha512
tar -xzf elasticsearch-7.17.10-linux-x86_64.tar.gz

# Elasticsearch cannot be run in root
# groupadd elasticsearch
# useradd elasticsearch -g elasticsearch -p elasticsearch
# chown -R elasticsearch:elasticsearch ./elasticsearch-7.17.10
# chmod o+x ./elasticsearch-7.17.10
# chgrp elasticsearch ./elasticsearch-7.17.10
# su - elasticsearch
adduser elasticsearch
chown -R elasticsearch elasticsearch-7.17.10
su elasticsearch
echo "--- starting elasticsearch"
timeout 20 elasticsearch-7.17.10/bin/elasticsearch -E http.port=9200
# check status of elasticsearch
curl http://127.0.0.1:9200
su root

echo "--- testing sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch_sink.slt'


echo "testing sink result"
# curl result is of the form
# {"took":3,"timed_out":false,"_shards": .. }, took varies from query and therefore
# we cut "{"took":3,"timed_out":false,"_shards":" out by awk.
diff ./e2e_test/sink/elasticsearch_sink.result \
<(curl -XGET "http://127.0.0.1:9200/test/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' \
 | awk -F"_shards\":{" '{print$2}' | awk -F"}}]}}" '{print $1}')
if [ $? -ne 0 ]; then
  echo "The output is not as expected."
  exit 1
fi
