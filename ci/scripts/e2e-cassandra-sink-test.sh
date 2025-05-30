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
wget --no-verbose $(get_latest_cassandra_download_url) -O cassandra_latest.tar.gz
tar xfvz cassandra_latest.tar.gz
export LATEST_CASSANDRA_VERSION=$(get_latest_cassandra_version)
export CASSANDRA_DIR="./apache-cassandra-${LATEST_CASSANDRA_VERSION}"

# Cassandra only support python 3.11
add-apt-repository ppa:deadsnakes/ppa
apt-get update
apt-get install -y software-properties-common
apt-get install -y python3.11
apt-get install -y python3.11-venv
python3.11 -m venv cqlsh_env
source cqlsh_env/bin/activate

export CQLSH_HOST=cassandra-server
export CQLSH_PORT=9042

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/cassandra_sink.slt'

deactivate

echo "--- Kill cluster"
cd ../../
risedev ci-kill
