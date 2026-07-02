#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

export MONGODB_URL="mongodb://mongodb:27017/?replicaSet=rs0"
export RISEDEV_MONGODB_WITH_OPTIONS_COMMON="connector='mongodb',mongodb.url='${MONGODB_URL}'"
export PATH="$(pwd)/e2e_test/commands:${PATH}"

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

echo "--- starting risingwave cluster"
cargo make ci-start ci-sink-test
sleep 1
if command -v docker >/dev/null 2>&1; then
    export MONGODB_CONTAINER="$(docker ps --filter label=com.docker.compose.service=mongodb --format '{{.Names}}' | head -n 1)"
fi

# install the mongo shell
wget --no-verbose http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
wget --no-verbose https://repo.mongodb.org/apt/ubuntu/dists/focal/mongodb-org/4.4/multiverse/binary-amd64/mongodb-org-shell_4.4.28_amd64.deb
dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb
dpkg -i mongodb-org-shell_4.4.28_amd64.deb

echo '> ping mongodb'
echo 'db.runCommand({ping: 1})' | mongo mongodb://mongodb:27017
echo '> rs config'
echo 'rs.conf()' | mongo mongodb://mongodb:27017
echo '> run mongodb sink test..'

sqllogictest -p 4566 -d dev './e2e_test/sink/mongodb_sink.slt'

echo "Mongodb sink check passed"

echo "--- Kill cluster"
risedev ci-kill
