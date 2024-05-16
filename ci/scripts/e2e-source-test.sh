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

echo "--- e2e, inline test"
risedev ci-start ci-inline-source-test
risedev slt './e2e_test/source_inline/**/*.slt'
echo "--- Kill cluster"
risedev ci-kill

echo "--- Prepare data"
cp src/connector/src/test_data/simple-schema.avsc ./avro-simple-schema.avsc
cp src/connector/src/test_data/complex-schema.avsc ./avro-complex-schema.avsc
cp src/connector/src/test_data/complex-schema ./proto-complex-schema
cp src/connector/src/test_data/complex-schema.json ./json-complex-schema


echo "--- e2e, ci-1cn-1fe, mysql & postgres cdc"

# import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc.sql

# import data to postgres
export PGHOST=db PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=cdc_test
createdb
psql < ./e2e_test/source/cdc/postgres_cdc.sql

echo "--- starting risingwave cluster"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-1cn-1fe-with-recovery

echo "--- mongodb cdc test"
# install the mongo shell
wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
wget https://repo.mongodb.org/apt/ubuntu/dists/focal/mongodb-org/4.4/multiverse/binary-amd64/mongodb-org-shell_4.4.28_amd64.deb
dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb
dpkg -i mongodb-org-shell_4.4.28_amd64.deb

echo '> ping mongodb'
echo 'db.runCommand({ping: 1})' | mongo mongodb://mongodb:27017
echo '> rs config'
echo 'rs.conf()' | mongo mongodb://mongodb:27017
echo '> run test..'
risedev slt './e2e_test/source/cdc/mongodb/**/*.slt'

echo "--- inline cdc test"
export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456
risedev slt './e2e_test/source/cdc_inline/**/*.slt'

echo "--- opendal source test"
risedev slt './e2e_test/source/opendal/**/*.slt'

echo "--- mysql & postgres cdc validate test"
risedev slt './e2e_test/source/cdc/cdc.validate.mysql.slt'
risedev slt './e2e_test/source/cdc/cdc.validate.postgres.slt'

echo "--- cdc share source test"
# cdc share stream test cases
export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456
risedev slt './e2e_test/source/cdc/cdc.share_stream.slt'

echo "--- mysql & postgres load and check"
risedev slt './e2e_test/source/cdc/cdc.load.slt'
# wait for cdc loading
sleep 10
risedev slt './e2e_test/source/cdc/cdc.check.slt'

# kill cluster
risedev kill
echo "> cluster killed "

echo "--- mysql & postgres recovery check"
# insert into mytest database (cdc.share_stream.slt)
mysql --protocol=tcp -u root mytest -e "INSERT INTO products
       VALUES (default,'RisingWave','Next generation Streaming Database'),
              (default,'Materialize','The Streaming Database You Already Know How to Use');
       UPDATE products SET name = 'RW' WHERE id <= 103;
       INSERT INTO orders VALUES (default, '2022-12-01 15:08:22', 'Sam', 1000.52, 110, false);"


# insert new rows
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source/cdc/mysql_cdc_insert.sql
echo "> inserted new rows into mysql"

psql < ./e2e_test/source/cdc/postgres_cdc_insert.sql
echo "> inserted new rows into postgres"

# start cluster w/o clean-data
unset RISINGWAVE_CI
export RUST_LOG="events::stream::message::chunk=trace,risingwave_stream=debug,risingwave_batch=info,risingwave_storage=info" \

risedev dev ci-1cn-1fe-with-recovery
echo "> wait for cluster recovery finish"
sleep 20
echo "> check mviews after cluster recovery"
# check results
risedev slt './e2e_test/source/cdc/cdc.check_new_rows.slt'

# drop relations
risedev slt './e2e_test/source/cdc/cdc_share_stream_drop.slt'

echo "--- Kill cluster"
risedev ci-kill

echo "--- e2e, ci-1cn-1fe, protobuf schema registry"
export RISINGWAVE_CI=true
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-1cn-1fe
python3 -m pip install --break-system-packages requests protobuf confluent-kafka
python3 e2e_test/schema_registry/pb.py "message_queue:29092" "http://message_queue:8081" "sr_pb_test" 20 user
echo "make sure google/protobuf/source_context.proto is NOT in schema registry"
curl --silent 'http://message_queue:8081/subjects'; echo
# curl --silent --head -X GET 'http://message_queue:8081/subjects/google%2Fprotobuf%2Fsource_context.proto/versions' | grep 404
curl --silent 'http://message_queue:8081/subjects' | grep -v 'google/protobuf/source_context.proto'
risedev slt './e2e_test/schema_registry/pb.slt'
risedev slt './e2e_test/schema_registry/alter_sr.slt'

echo "--- Kill cluster"
risedev ci-kill

echo "--- e2e, ci-kafka-plus-pubsub, kafka and pubsub source"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-kafka
./scripts/source/prepare_ci_kafka.sh
risedev slt './e2e_test/source/basic/*.slt'
risedev slt './e2e_test/source/basic/old_row_format_syntax/*.slt'
risedev slt './e2e_test/source/basic/alter/kafka.slt'

echo "--- e2e, kafka alter source rate limit"
risedev slt './e2e_test/source/basic/alter/rate_limit_source_kafka.slt'
risedev slt './e2e_test/source/basic/alter/rate_limit_table_kafka.slt'

echo "--- e2e, kafka alter source"
chmod +x ./scripts/source/prepare_data_after_alter.sh
./scripts/source/prepare_data_after_alter.sh 2
risedev slt './e2e_test/source/basic/alter/kafka_after_new_data.slt'

echo "--- e2e, kafka alter source again"
./scripts/source/prepare_data_after_alter.sh 3
risedev slt './e2e_test/source/basic/alter/kafka_after_new_data_2.slt'

echo "--- Run CH-benCHmark"
risedev slt './e2e_test/ch_benchmark/batch/ch_benchmark.slt'
risedev slt './e2e_test/ch_benchmark/streaming/*.slt'
