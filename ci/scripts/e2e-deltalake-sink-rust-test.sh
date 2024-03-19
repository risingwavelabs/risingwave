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
cargo make ci-start ci-sink-test
sleep 1

# prepare minio deltalake sink
echo "--- preparing deltalake"
.risingwave/bin/mcli -C .risingwave/config/mcli mb hummock-minio/deltalake
wget https://rw-ci-deps-dist.s3.amazonaws.com/spark-3.3.1-bin-hadoop3.tgz
tar -xf spark-3.3.1-bin-hadoop3.tgz --no-same-owner
DEPENDENCIES=io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.2
spark-3.3.1-bin-hadoop3/bin/spark-sql --packages $DEPENDENCIES \
    --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
    --conf 'spark.hadoop.fs.s3a.access.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.secret.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9301' \
    --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
    --S --e 'create table delta.`s3a://deltalake/deltalake-test`(v1 int, v2 short, v3 long, v4 float, v5 double, v6 string, v7 date, v8 Timestamp, v9 boolean) using delta;'


echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/deltalake_rust_sink.slt'
sleep 1


spark-3.3.1-bin-hadoop3/bin/spark-sql --packages $DEPENDENCIES \
    --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
    --conf 'spark.hadoop.fs.s3a.access.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.secret.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.endpoint=http://localhost:9301' \
    --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
    --S --e 'INSERT OVERWRITE DIRECTORY "./spark-output" USING CSV SELECT * FROM delta.`s3a://deltalake/deltalake-test`;'

# check sink destination using shell
if cat ./spark-output/*.csv | sort | awk -F "," '{
    exit !($1 == 1 && $2 == 1 && $3 == 1 && $4 == 1.1 && $5 == 1.2 && $6 == "test" && $7 == "2013-01-01" && $8 == "2013-01-01T01:01:01.000Z" && $9 == "false"); }'; then
  echo "DeltaLake sink check passed"
else
  cat ./spark-output/*.csv
  echo "The output is not as expected."
  exit 1
fi

echo "--- Kill cluster"
cargo make ci-kill