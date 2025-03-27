#!/usr/bin/env bash

set -e

# stop if already running
if nc -z localhost 15002; then
    echo "Spark connect server already running at port 15002"
    exit 0
fi

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR"

# check java version, only 8/11/17 are supported
if type -p java; then
    JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
    if [[ "$JAVA_VER" != "8" && "$JAVA_VER" != "11" && "$JAVA_VER" != "17" ]]; then
        echo -e "\e[31mOnly Java 8/11/17 are supported. Current version: $JAVA_VER\e[0m"
        exit 1
    fi
else
    echo -e "\e[31mJava not found. Please install Java 8/11/17.\e[0m"
    exit 1
fi

# https://iceberg.apache.org/releases/
# https://spark.apache.org/downloads.html
ICEBERG_VERSION=1.8.1
SPARK_VERSION=3.5.5

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$ICEBERG_VERSION,org.apache.hadoop:hadoop-aws:3.3.2"
PACKAGES="$PACKAGES,org.apache.spark:spark-connect_2.12:$SPARK_VERSION"

SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz"

if [ ! -d "spark-${SPARK_VERSION}-bin-hadoop3" ];then
    wget --no-verbose https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/$SPARK_FILE
    tar -xzf $SPARK_FILE --no-same-owner
fi

# start new server
./spark-${SPARK_VERSION}-bin-hadoop3/sbin/start-connect-server.sh --packages $PACKAGES \
  --master local[3] \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=hadoop \
  --conf spark.sql.catalog.demo.warehouse=s3a://icebergdata/demo \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://127.0.0.1:9301 \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.path.style.access=true \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=hummockadmin \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=hummockadmin \
  --conf spark.sql.defaultCatalog=demo

echo "Waiting spark connector server to launch on 15002..."

while ! nc -z localhost 15002; do
  sleep 1 # wait for 1/10 of the second before check again
done

echo "Spark connect server launched"
