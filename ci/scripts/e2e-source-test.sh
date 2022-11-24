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

echo "--- e2e, ci-1cn-1fe, cdc source"
# install mysql client
apt-get -y install mysql-client
# import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc.sql
# start risingwave cluster
cargo make ci-start ci-1cn-1fe
# start cdc connector node
nohup java -cp ./connector-service.jar com.risingwave.sourcenode.service.SourceServiceMain > .risingwave/log/connector-source.log 2>&1 &
sleep 1
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.load.slt'
# wait for cdc loading
sleep 4
sqllogictest -p 4566 -d dev './e2e_test/source/cdc/cdc.check.slt'

echo "--- Kill cluster"
pkill -f connector-service.jar
cargo make ci-kill

echo "--- e2e test w/ Rust frontend - source with kafka"
cargo make clean-data
cargo make ci-start ci-kafka
./scripts/source/prepare_ci_kafka.sh
sqllogictest -p 4566 -d dev  './e2e_test/source/basic/*.slt'


echo "--- Run CH-benCHmark"
./risedev slt -p 4566 -d dev ./e2e_test/ch-benchmark/ch_benchmark.slt
