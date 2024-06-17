#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_LIBS_PATH="./connector-node/libs"

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

download_and_prepare_rw "$profile" source

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

echo "--- starting risingwave cluster"
risedev ci-start ci-sink-test
# Wait cassandra server to start
sleep 40

echo "--- create cassandra table"
curl https://downloads.apache.org/cassandra/4.1.3/apache-cassandra-4.1.3-bin.tar.gz  --output apache-cassandra-4.1.3-bin.tar.gz
tar xfvz apache-cassandra-4.1.3-bin.tar.gz
# remove bundled packages, and use installed packages, because Python 3.12 has removed asyncore, but I failed to install libev support for bundled Python driver.
rm apache-cassandra-4.1.3/lib/six-1.12.0-py2.py3-none-any.zip
rm apache-cassandra-4.1.3/lib/cassandra-driver-internal-only-3.25.0.zip
apt-get install -y libev4 libev-dev
pip3 install --break-system-packages cassandra-driver

cd apache-cassandra-4.1.3/bin
export CQLSH_HOST=cassandra-server
export CQLSH_PORT=9042
./cqlsh --request-timeout=20 -e "CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};use demo;
CREATE table demo_bhv_table(v1 int primary key,v2 smallint,v3 bigint,v4 float,v5 double,v6 text,v7 date,v8 timestamp,v9 boolean);"

echo "--- testing sinks"
cd ../../
sqllogictest -p 4566 -d dev './e2e_test/sink/cassandra_sink.slt'
sleep 1
cd apache-cassandra-4.1.3/bin
./cqlsh --request-timeout=20 -e "COPY demo.demo_bhv_table TO './query_result.csv' WITH HEADER = false AND ENCODING = 'UTF-8';"

if cat ./query_result.csv | awk -F "," '{
    exit !($1 == 1 && $2 == 1 && $3 == 1 && $4 == 1.1 && $5 == 1.2 && $6 == "test" && $7 == "2013-01-01" && $8 == "2013-01-01 01:01:01.000+0000" && $9 == "False\r"); }'; then
  echo "Cassandra sink check passed"
else
  echo "The output is not as expected."
  echo "output:"
  cat ./query_result.csv
  exit 1
fi

echo "--- Kill cluster"
cd ../../
risedev ci-kill