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

download_and_prepare_rw "$profile" source

echo "--- starting risingwave cluster"
PGPASSWORD=postgres psql -h db -p 5432 -U postgres -c "DROP DATABASE IF EXISTS metadata;" -c "CREATE DATABASE metadata;"
risedev ci-start ci-iceberg-test

# prepare minio iceberg sink
echo "--- preparing iceberg"
.risingwave/bin/mcli -C .risingwave/config/mcli mb hummock-minio/icebergdata

cd e2e_test/iceberg
bash ./start_spark_connect_server.sh

echo "--- Running tests"
# Don't remove the `--quiet` option since poetry has a bug when printing output, see
# https://github.com/python-poetry/poetry/issues/3412
poetry update --quiet
poetry run python main.py -t ./test_case/no_partition_append_only.toml
poetry run python main.py -t ./test_case/no_partition_upsert.toml
poetry run python main.py -t ./test_case/partition_append_only.toml
poetry run python main.py -t ./test_case/partition_upsert.toml
poetry run python main.py -t ./test_case/range_partition_append_only.toml
poetry run python main.py -t ./test_case/range_partition_upsert.toml
poetry run python main.py -t ./test_case/append_only_with_checkpoint_interval.toml
poetry run python main.py -t ./test_case/iceberg_select_empty_table.toml
poetry run python main.py -t ./test_case/iceberg_source_equality_delete.toml
poetry run python main.py -t ./test_case/iceberg_source_position_delete.toml
poetry run python main.py -t ./test_case/iceberg_source_all_delete.toml
poetry run python main.py -t ./test_case/iceberg_source_explain_for_delete.toml
poetry run python main.py -t ./test_case/iceberg_predicate_pushdown.toml
poetry run python main.py -t ./test_case/iceberg_connection.toml

echo "--- Running benchmarks"
poetry run python main.py -t ./benches/predicate_pushdown.toml

echo "--- Running iceberg engine tests"
sqllogictest -p 4566 -d dev './e2e_test/iceberg/test_case/iceberg_engine.slt'

echo "--- Legacy test"
DEPENDENCIES=org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.2
spark-3.3.1-bin-hadoop3/bin/spark-sql --packages $DEPENDENCIES \
    --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.demo.type=hadoop \
    --conf spark.sql.catalog.demo.warehouse=s3a://iceberg/ \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://127.0.0.1:9301 \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=hummockadmin \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=hummockadmin \
    --S --e "CREATE TABLE demo.demo_db.e2e_demo_table(v1 int, v2 bigint, v3 string) TBLPROPERTIES ('format-version'='2');"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/iceberg_sink.slt'
sleep 1

# check sink destination iceberg
spark-3.3.1-bin-hadoop3/bin/spark-sql --packages $DEPENDENCIES \
    --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.demo.type=hadoop \
    --conf spark.sql.catalog.demo.warehouse=s3a://iceberg/ \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://127.0.0.1:9301 \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=hummockadmin \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=hummockadmin \
    --S --e "INSERT OVERWRITE DIRECTORY './spark-output' USING CSV SELECT * FROM demo.demo_db.e2e_demo_table;"

# check sink destination using shell
if cat ./spark-output/*.csv | sort | awk -F "," '{
if ($1 == 1 && $2 == 50 && $3 == "1-50") c1++;
 if ($1 == 13 && $2 == 2 && $3 == "13-2") c2++;
  if ($1 == 21 && $2 == 2 && $3 == "21-2") c3++;
   if ($1 == 2 && $2 == 2 && $3 == "2-2") c4++;
    if ($1 == 3 && $2 == 2 && $3 == "3-2") c5++;
     if ($1 == 5 && $2 == 2 && $3 == "5-2") c6++;
      if ($1 == 8 && $2 == 2 && $3 == "8-2") c7++; }
       END { exit !(c1 == 1 && c2 == 1 && c3 == 1 && c4 == 1 && c5 == 1 && c6 == 1 && c7 == 1); }'; then
  echo "Iceberg sink check passed"
else
  echo "The output is not as expected."
  exit 1
fi

spark-3.3.1-bin-hadoop3/bin/spark-sql --packages $DEPENDENCIES \
    --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.demo.type=hadoop \
    --conf spark.sql.catalog.demo.warehouse=s3a://iceberg/ \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://127.0.0.1:9301 \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=hummockadmin \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=hummockadmin \
    --S --e "drop table demo.demo_db.e2e_demo_table;"



echo "--- Legacy test (CDC)"

# 1. import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./test_case/cdc/mysql_cdc.sql

# 2. create table and sink
poetry run python main.py -t ./test_case/cdc/no_partition_cdc_init.toml

# 3. insert new data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./test_case/cdc/mysql_cdc_insert.sql

sleep 20

# 4. check change
poetry run python main.py -t ./test_case/cdc/no_partition_cdc.toml


echo "--- Kill cluster"
cd ../../
risedev ci-kill
