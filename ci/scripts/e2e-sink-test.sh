#!/usr/bin/env bash

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

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node


echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-dev

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.source.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
cargo make pre-start-dev
cargo make link-all-in-one-binaries

# prepare environment mysql sink
mysql --host=mysql --port=3306 -u root -p123456 -e "CREATE DATABASE IF NOT EXISTS test;"
# grant access to `test` for ci test user
mysql --host=mysql --port=3306 -u root -p123456 -e "GRANT ALL PRIVILEGES ON test.* TO 'mysqluser'@'%';"
# create a table named t_remote
mysql --host=mysql --port=3306 -u root -p123456 -e "CREATE TABLE IF NOT EXISTS test.t_remote (id INT, name VARCHAR(255), PRIMARY KEY (id));"

echo "--- preparing postgresql"

# set up PG sink destination
apt-get -y install postgresql-client
export PGPASSWORD=postgres
psql -h db -U postgres -c "CREATE ROLE test LOGIN SUPERUSER PASSWORD 'connector';"
createdb -h db -U postgres test
psql -h db -U postgres -d test -c "CREATE TABLE t4 (v1 int PRIMARY KEY, v2 int);"
psql -h db -U postgres -d test -c "CREATE TABLE t_remote (id serial PRIMARY KEY, name VARCHAR (50) NOT NULL);"

node_port=50051
node_timeout=10
./connector-node/start-service.sh -p $node_port > .risingwave/log/connector-source.log 2>&1 &
echo "waiting for connector node to start"
start_time=$(date +%s)
while :
do
    if nc -z localhost $node_port; then
        echo "Port $node_port is listened! Connector Node is up!"
        break
    fi

    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))
    if [ $elapsed_time -ge $node_timeout ]; then
        echo "Timeout waiting for port $node_port to be listened!"
        exit 1
    fi
    sleep 0.1
done

echo "--- starting risingwave cluster with connector node"
cargo make ci-start ci-1cn-1fe

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/append_only_sink.slt'
sqllogictest -p 4566 -d dev './e2e_test/sink/create_sink_as.slt'
sqllogictest -p 4566 -d dev './e2e_test/sink/blackhole_sink.slt'
sleep 1

# check sink destination postgres
sqllogictest -p 4566 -d dev './e2e_test/sink/remote/jdbc.load.slt'
sleep 1
sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/jdbc.check.pg.slt'
sleep 1

# check sink destination mysql using shell
if mysql  --host=mysql --port=3306 -u root -p123456 -sN -e "SELECT * FROM test.t_remote ORDER BY id;" | awk '{
if ($1 == 1 && $2 == "Alex") c1++;
 if ($1 == 3 && $2 == "Carl") c2++;
  if ($1 == 4 && $2 == "Doris") c3++;
   if ($1 == 5 && $2 == "Eve") c4++;
    if ($1 == 6 && $2 == "Frank") c5++; }
     END { exit !(c1 == 1 && c2 == 1 && c3 == 1 && c4 == 1 && c5 == 1); }'; then
  echo "mysql sink check passed"
else
  echo "The output is not as expected."
  exit 1
fi

echo "--- Kill cluster"
pkill -f connector-node
cargo make ci-kill
