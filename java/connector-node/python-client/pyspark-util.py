# Copyright 2023 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
from pyspark.sql import SparkSession, Row

def init_iceberg_spark():
    return SparkSession.builder.master("local").config(
        'spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.3.2').config(
        'spark.sql.catalog.demo', 'org.apache.iceberg.spark.SparkCatalog').config(
        'spark.sql.catalog.demo.type', 'hadoop').config(
        'spark.sql.catalog.demo.warehouse', 's3a://bucket/').config(
        'spark.sql.catalog.demo.hadoop.fs.s3a.endpoint', 'http://127.0.0.1:9000').config(
        'spark.sql.catalog.demo.hadoop.fs.s3a.access.key', 'minioadmin').config(
        'spark.sql.catalog.demo.hadoop.fs.s3a.secret.key', 'minioadmin').getOrCreate()

def init_deltalake_spark():
    return SparkSession.builder.master("local").config(
        'spark.jars.packages', 'io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.2').config(
        'spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension').config(
        'spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').config(
        'spark.hadoop.fs.s3a.endpoint', 'http://127.0.0.1:9000').config(
        'spark.hadoop.fs.s3a.access.key', 'minioadmin').config(
        'spark.hadoop.fs.s3a.secret.key', 'minioadmin').getOrCreate()

def create_iceberg_table():
    spark = init_iceberg_spark()
    spark.sql("create table demo.demo_db.demo_table(id int, name string) TBLPROPERTIES ('format-version'='2');")
    print("Table demo.demo_db.demo_table(id int, name string) created")

def drop_iceberg_table():
    spark = init_iceberg_spark()
    spark.sql("drop table demo.demo_db.demo_table;")
    print("Table demo.demo_db.demo_table dropped")

def read_iceberg_table():
    spark = init_iceberg_spark()
    spark.sql("select * from demo.demo_db.demo_table;").show()

def test_table(input_file, actual_list):
    actual = []
    for row in actual_list:
        actual.append(row.asDict())
    actual = sorted(actual, key = lambda ele: sorted(ele.items()))

    with open(input_file, 'r') as file:
        sink_input = json.load(file)
    expected = []
    for batch in sink_input:
        for row in batch:
            expected.append(row)
    expected = sorted(expected, key = lambda ele: sorted(ele.items()))

    if actual == expected:
        print("Test passed")
    else:
        print("Expected:", expected, "\nActual:", actual)
        raise Exception("Test failed")

def test_iceberg_table(input_file):
    spark = init_iceberg_spark()
    list = spark.sql("select * from demo.demo_db.demo_table;").collect()
    test_table(input_file, list)
    
def test_upsert_iceberg_table(input_file):
    spark = init_iceberg_spark()
    list = spark.sql("select * from demo.demo_db.demo_table;").collect()
    actual = []
    for row in list:
        actual.append(row.asDict())
    actual = sorted(actual, key = lambda ele: sorted(ele.items()))

    with open(input_file, 'r') as file:
        sink_input = json.load(file)

    expected = []
    for batch in sink_input:
        for row in batch:
            match row['op_type']:
                case 1:
                    expected.append(row['line'])
                case 2:
                    expected.remove(row['line'])
                case 3:
                    expected.append(row['line'])
                case 4:
                    expected.remove(row['line'])
                case _:
                    raise Exception("Unknown op_type")

    expected = sorted(expected, key = lambda ele: sorted(ele.items()))

    if actual == expected:
        print("Test passed")
    else:
        print("Expected:", expected, "\nActual:", actual)
        raise Exception("Test failed")

def read_deltalake_table():
    spark = init_deltalake_spark()
    spark.sql("select * from delta.`s3a://bucket/delta`;").show()

def create_deltalake_table():
    spark = init_deltalake_spark()
    spark.sql("create table delta.`s3a://bucket/delta`(id int, name string) using delta;")
    print("Table delta.`s3a://bucket/delta`(id int, name string) created")

def test_deltalake_table(input_file):
    spark = init_deltalake_spark()
    list = spark.sql("select * from delta.`s3a://bucket/delta`;").collect()
    test_table(input_file, list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('operation', help="operation on table: read, create, drop, test or test_upsert")
    parser.add_argument('--input_file', default="./data/sink_input.json", help="input data to run tests")
    args = parser.parse_args()
    match args.operation:
        case "read_iceberg":
            read_iceberg_table()
        case "create_iceberg":
            create_iceberg_table()
        case "drop_iceberg":
            drop_iceberg_table()
        case "test_iceberg":
            test_iceberg_table(args.input_file)
        case "test_upsert_iceberg":
            test_upsert_iceberg_table(args.input_file)

        case "read_deltalake":
            read_deltalake_table()
        case "create_deltalake":
            create_deltalake_table()
        case "test_deltalake":
            test_deltalake_table(args.input_file)
        case _:
            raise Exception("Unknown operation")
