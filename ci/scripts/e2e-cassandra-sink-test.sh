#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

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

cassandra_version="4.0.21"
cassandra_archive="apache-cassandra-${cassandra_version}-bin.tar.gz"

sink_test_env_setup "$profile" --need-connector --sleep-duration 40

echo "--- install Cassandra client"
wget --no-verbose "https://archive.apache.org/dist/cassandra/${cassandra_version}/${cassandra_archive}" -O "$cassandra_archive"
tar xfvz "$cassandra_archive"
export CASSANDRA_DIR="./apache-cassandra-${cassandra_version}"

# Cassandra only support python 3.11
add-apt-repository ppa:deadsnakes/ppa
apt-get update
apt-get install -y software-properties-common
apt-get install -y python3.11
apt-get install -y python3.11-venv
python3.11 -m venv cqlsh_env
source cqlsh_env/bin/activate

export CQLSH_PYTHON="${VIRTUAL_ENV}/bin/python3.11"
export CQLSH_HOST=cassandra-server
export CQLSH_PORT=9042
export CASSANDRA_DATACENTER=datacenter1
export RISEDEV_CASSANDRA_WITH_OPTIONS_COMMON="connector='cassandra',cassandra.url='${CQLSH_HOST}:${CQLSH_PORT}',cassandra.datacenter='${CASSANDRA_DATACENTER}'"
export PATH="${PWD}/e2e_test/commands:${PATH}"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/cassandra_sink.slt'

deactivate

echo "--- Kill cluster"
risedev ci-kill
