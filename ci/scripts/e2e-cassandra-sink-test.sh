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

echo "--- install cassandra"
wget $(get_latest_cassandra_download_url)  --output cassandra_latest.tar.gz
tar xfvz cassandra_latest.tar.gz
LATEST_CASSANDRA_VERSION=$(get_latest_cassandra_version)
CASSANDRA_DIR="./apache-cassandra-${LATEST_CASSANDRA_VERSION}"
# remove bundled packages, and use installed packages, because Python 3.12 has removed asyncore, but I failed to install libev support for bundled Python driver.
rm ${CASSANDRA_DIR}/lib/six-1.12.0-py2.py3-none-any.zip
rm ${CASSANDRA_DIR}/lib/cassandra-driver-internal-only-3.25.0.zip
apt-get install -y libev4 libev-dev
pip3 install --break-system-packages cassandra-driver
export CQLSH_HOST=cassandra-server
export CQLSH_PORT=9042

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/cassandra_sink.slt'

if cat ./query_result.csv | awk -F "," '{
    exit !($1 == 1 && $2 == 1 && $3 == 1 && $4 == 1.1 && $5 == 1.2 && $6 == "test" && $7 == "2013-01-01" && $8 == "2013-01-01 01:01:01.000+0000" && $9 == "False\r"); }'; then
  echo "Cassandra sink check passed"
else
  echo "The output is not as expected."
  echo "output:"
  cat ./query_result.csv
  exit 1
fi

if cat ./query_result2.csv | awk -F "," '{
    exit !($1 == 1 && $2 == 1 && $3 == "1\r"); }'; then
  echo "Cassandra sink check passed"
else
  echo "The output is not as expected."
  echo "output:"
  cat ./query_result2.csv
  exit 1
fi

echo "--- Kill cluster"
cd ../../
risedev ci-kill