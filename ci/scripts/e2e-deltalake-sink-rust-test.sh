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

sink_test_env_setup "$profile" --need-connector

# prepare minio deltalake sink
echo "--- preparing deltalake"
risedev mc mb hummock-minio/deltalake
SPARK_VERSION=4.0.2
DELTA_VERSION=4.0.1
SPARK_DIR="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_FILE="${SPARK_DIR}.tgz"

if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA_BIN="${JAVA_HOME}/bin/java"
else
    JAVA_BIN="$(type -p java || true)"
fi

if [[ -x "$JAVA_BIN" ]]; then
    JAVA_VER=$("$JAVA_BIN" -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
    if [[ "$JAVA_VER" != "17" && "$JAVA_VER" != "21" ]]; then
        echo -e "\e[31mOnly Java 17/21 are supported for Spark 4.0. Current version: $JAVA_VER\e[0m"
        exit 1
    fi
else
    echo -e "\e[31mJava not found. Please install Java 17 or 21.\e[0m"
    exit 1
fi

if [ ! -d "$SPARK_DIR" ]; then
    wget --no-verbose "https://rw-ci-deps-dist.s3.amazonaws.com/${SPARK_FILE}"
    tar -xzf "$SPARK_FILE" --no-same-owner
fi
DEPENDENCIES="io.delta:delta-spark_2.13:${DELTA_VERSION},org.apache.hadoop:hadoop-aws:3.4.1"
unset SPARK_HOME

"${SPARK_DIR}/bin/spark-sql" --packages "$DEPENDENCIES" \
    --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
    --conf 'spark.sql.session.timeZone=UTC' \
    --conf 'spark.hadoop.fs.s3a.access.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.secret.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9301' \
    --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
    --S --e '
        create table delta.`s3a://deltalake/deltalake-test`(
            v1 int, v2 short, v3 long, v4 float, v5 double, v6 string, v7 date, v8 Timestamp, v9 boolean, v10 decimal, v11 ARRAY<decimal>
        ) using delta;
        create table delta.`s3a://deltalake/deltalake-test-exactly-once`(
            v1 int, v2 short, v3 long, v4 float, v5 double, v6 string, v7 date, v8 Timestamp, v9 boolean, v10 decimal, v11 ARRAY<decimal>
        ) using delta;
    '


echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/deltalake_rust_sink.slt'
sleep 1


check_delta_table() {
    local table_path="$1"
    local output_dir="$2"

    rm -rf "$output_dir"
    "${SPARK_DIR}/bin/spark-sql" --packages "$DEPENDENCIES" \
        --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
        --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
        --conf 'spark.sql.session.timeZone=UTC' \
        --conf 'spark.hadoop.fs.s3a.access.key=hummockadmin' \
        --conf 'spark.hadoop.fs.s3a.secret.key=hummockadmin' \
        --conf 'spark.hadoop.fs.s3a.endpoint=http://localhost:9301' \
        --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
        --S --e "INSERT OVERWRITE DIRECTORY \"$output_dir\" USING CSV SELECT v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,CAST(v11 as varchar(12)) FROM delta.\`$table_path\`;"

    if cat "$output_dir"/*.csv | sort | awk -F "," '{
        exit !($1 == 1 && $2 == 1 && $3 == 1 && $4 == 1.1 && $5 == 1.2 && $6 == "test" && $7 == "2013-01-01" && $8 == "2013-01-01T01:01:01.000Z" && $9 == "false" && $10 == 1 && $11 == "[1]"); }'; then
      echo "DeltaLake sink check passed for $table_path"
    else
      cat "$output_dir"/*.csv
      echo "The output is not as expected for $table_path."
      exit 1
    fi
}

check_delta_table 's3a://deltalake/deltalake-test' './spark-output'
check_delta_table 's3a://deltalake/deltalake-test-exactly-once' './spark-output-exactly-once'

echo "--- Kill cluster"
risedev ci-kill
