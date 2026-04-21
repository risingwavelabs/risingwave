#! /usr/bin/env bash

# https://iceberg.apache.org/releases/
# https://spark.apache.org/downloads.html
ICEBERG_VERSION=1.10.1
SPARK_VERSION=4.0.2

SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:$ICEBERG_VERSION,org.apache.hadoop:hadoop-aws:3.4.1"
SPARK_PACKAGES="$SPARK_PACKAGES,org.apache.spark:spark-connect_2.13:$SPARK_VERSION"
