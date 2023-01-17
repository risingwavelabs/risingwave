#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

# prepare environment
export CONNECTOR_RPC_ENDPOINT="localhost:60061"

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

echo "--- Prepare data"
cp src/source/src/test_data/simple-schema.avsc ./avro-simple-schema.avsc
cp src/source/src/test_data/complex-schema.avsc ./avro-complex-schema.avsc
cp src/source/src/test_data/complex-schema ./proto-complex-schema

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-dev

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.source.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
cargo make pre-start-dev
cargo make link-all-in-one-binaries

echo "--- e2e, ci-1cn-1fe, mysql & postgres cdc"
# install mysql client
apt-get -y install mysql-client
# import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc.sql

# import data to postgres
export PGPASSWORD='postgres';
createdb -h db -U postgres cdc_test
psql -h db -U postgres -d cdc_test < ./e2e_test/source/cdc/postgres_cdc.sql

# start cdc connector node
nohup java -jar ./connector-service.jar --port 60061 > .risingwave/log/connector-node.log 2>&1 &
# start risingwave cluster
cargo make ci-start ci-1cn-1fe-with-recovery
sleep 2

echo "---- mysql & postgres cdc validate test"
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.validate.mysql.slt'
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.validate.postgres.slt'

echo "---- mysql & postgres load and check"
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.load.slt'
# wait for cdc loading
sleep 10
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.check.slt'

# kill cluster
cargo make kill
# insert new rows
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc_insert.sql
psql -h db -U postgres -d cdc_test < ./e2e_test/source/cdc/postgres_cdc_insert.sql

# start cluster w/o clean-data
cargo make dev ci-1cn-1fe-with-recovery
echo "wait for recovery finish"
sleep 10
echo "check mviews after cluster recovery"
# check results
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.check_new_rows.slt'

echo "--- Kill cluster"
pkill -f connector-service.jar
cargo make ci-kill

echo "--- e2e, ci-1cn-1fe, mysql & pg longevity demo (load snapshot)"
# install mysql client if needed
# apt-get -y install mysql-client

# install go for go-tpc tool
apt-get -y install golang-1.18

# start cdc connector node
nohup java -jar ./connector-service.jar --port 60061 > .risingwave/log/connector-node.log 2>&1 &

# start risingwave cluster
cargo make ci-start ci-1cn-1fe-with-recovery

# download and compile go-tpc
git clone https://github.com/pingcap/go-tpc.git
cd go-tpc
make build

# generate data to mysql or pg
# -H database host, -P database port, -D database name, -T number threads
# refer to `go-tpc tpcc --help` for details of those arguments
./bin/go-tpc tpcc prepare --no-check true --warehouses 1 -T 4 -H mysql -U root -p '123456' -D test -P 3306

# generate data to postgres
./bin/go-tpc tpcc prepare --no-check true --warehouses 1 -T 4 -d postgres -U postgres -p 'postgres' -H db -D test -P 5432 --conn-params sslmode=disable

cd ..

# create cdc source to load specified table data into risingwave
# table schema definitions https://github.com/pingcap/go-tpc/blob/97009c9b58/tpcc/ddl.go
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.demo.load.slt'

# wait for cdc loading
# depends on the size of data, it may take a while to load it from mysql
sleep 15

# On first stage, we can just check if the number of rows are the same in mysql and risingwave.
# We can also check the data correctness of tables in risingwave if the go-tpc can run on risingwave.
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.demo.check.slt'

echo "--- Kill cluster"
pkill -f connector-service.jar
cargo make ci-kill


echo "--- e2e, ci-1cn-1fe, mysql & pg longevity demo (consume cdc events)"
# install mysql client if needed
# apt-get -y install mysql-client

# install go for go-tpc tool
# apt-get -y install golang-1.18

# start cdc connector node
nohup java -jar ./connector-service.jar --port 60061 > .risingwave/log/connector-node.log 2>&1 &

# start risingwave cluster
cargo make ci-start ci-1cn-1fe-with-recovery
sleep 2

# create empty tables in the source databsae
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/demo_mysql_cdc_create.sql
createdb -h db -U postgres test
psql -h db -U postgres -d test < ./e2e_test/source/cdc/demo_postgres_cdc_create.sql

# create cdc sources in risingwave first
# Afterwards, if there are new rows inserted into the source tables,
# cdc events will push to risingwave.
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.demo.load.slt'

# download and compile go-tpc
# git clone https://github.com/pingcap/go-tpc.git
# cd go-tpc
# make build

# ingest data to mysql or pg
# -H database host, -P database port, -D database name, -T number threads
# refer to `go-tpc tpcc --help` for details of those arguments
./bin/go-tpc tpcc prepare --no-check true --warehouses 1 -T 4 -H mysql -U root -p '123456' -D test -P 3306

# ingest data to postgres
./bin/go-tpc tpcc prepare --no-check true --warehouses 1 -T 4 -d postgres -U postgres -p 'postgres' -H db -D test -P 5432 --conn-params sslmode=disable

cd ..

# wait for cdc loading
# depends on the size of data, it may take a while to load it from mysql
sleep 15

# On first stage, we can just check if the number of rows are the same in mysql and risingwave.
# We can also check the data correctness of tables in risingwave if the go-tpc can run on risingwave.
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.demo.check.slt'

echo "--- Kill cluster"
pkill -f connector-service.jar
cargo make ci-kill


echo "--- e2e, ci-1cn-1fe, nexmark endless"
cargo make ci-start ci-1cn-1fe
sqllogictest -p 4566 -d dev './e2e_test/source/nexmark_endless/*.slt'

echo "--- Kill cluster"
cargo make ci-kill


echo "--- e2e, ci-kafka-plus-pubsub, kafka and pubsub source"
cargo make ci-start ci-kafka-plus-pubsub
./scripts/source/prepare_ci_kafka.sh
cargo run --bin prepare_ci_pubsub
sqllogictest -p 4566 -d dev './e2e_test/source/basic/*.slt'

echo "--- Run CH-benCHmark"
./risedev slt -p 4566 -d dev './e2e_test/ch_benchmark/batch/ch_benchmark.slt'
./risedev slt -p 4566 -d dev './e2e_test/ch_benchmark/streaming/*.slt'
