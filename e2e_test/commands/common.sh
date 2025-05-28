#! /usr/bin/env bash

# https://iceberg.apache.org/releases/
# https://spark.apache.org/downloads.html
ICEBERG_VERSION=1.8.1
SPARK_VERSION=3.5.6

SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$ICEBERG_VERSION,org.apache.hadoop:hadoop-aws:3.3.2"
SPARK_PACKAGES="$SPARK_PACKAGES,org.apache.spark:spark-connect_2.12:$SPARK_VERSION"
