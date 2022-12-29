#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

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

echo "--- Download artifacts"
mkdir -p target/debug
buildkite-agent artifact download risingwave-"$profile" target/debug/
buildkite-agent artifact download risedev-dev-"$profile" target/debug/
mv target/debug/risingwave-"$profile" target/debug/risingwave
mv target/debug/risedev-dev-"$profile" target/debug/risedev-dev

echo "--- Download connector node jar"
buildkite-agent artifact download connector-service.jar ./

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-dev

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.source.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
cargo make pre-start-dev
cargo make link-all-in-one-binaries

# prepare environment mysql sink
apt-get -y install mysql-client
mysql --host=mysql --port=3306 -u root -p123456 -e "CREATE DATABASE IF NOT EXISTS test;"

echo "--- preparing postgresql"

# set up PG sink destination
apt-get -y install postgresql-client
export PGPASSWORD=postgres
psql -h db -U postgres -c "CREATE ROLE test LOGIN SUPERUSER PASSWORD 'connector';"
createdb -h db -U postgres test
psql -h db -U postgres -d test -c "CREATE TABLE t4 (v1 int, v2 int);"
psql -h db -U postgres -d test -c "CREATE TABLE t_remote (id serial PRIMARY KEY, name VARCHAR (50) NOT NULL);"

echo "--- starting risingwave cluster with connector node"
cargo make ci-start ci-1cn-1fe
java -jar ./connector-service.jar --port 60061 > .risingwave/log/connector-source.log 2>&1 &
sleep 1

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/*.slt'
sleep 1
sqllogictest -p 4566 -d dev './e2e_test/sink/remote/remote.load.slt'
sleep 1

# check sink destination
sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/remote.check.slt'

echo "--- Kill cluster"
pkill -f connector-service.jar
cargo make ci-kill
