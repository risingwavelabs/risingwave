from pyspark.sql import SparkSession
import argparse
import configparser
import subprocess
import csv
import unittest
import time
import tomli as toml
from datetime import date
from datetime import datetime
from datetime import timezone


def strtobool(v):
    return v.lower() == 'true'


def strtodate(v):
    return date.fromisoformat(v)


def strtots(v):
    return datetime.fromisoformat(v).astimezone(timezone.utc).replace(tzinfo=None)


g_spark = None


def get_spark(args):
    spark_config = args['spark']
    global g_spark
    if g_spark is None:
        g_spark = SparkSession.builder.remote(spark_config['url']).getOrCreate()

    return g_spark


def init_iceberg_table(args,init_sqls):
    spark = get_spark(args)
    for sql in init_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)


def init_risingwave_mv(args,slt):
    rw_config = args['risingwave']
    cmd = f"sqllogictest -p {rw_config['port']} -d {rw_config['db']} {slt}"
    print(f"Command line is [{cmd}]")
    subprocess.run(cmd,
                   shell=True,
                   check=True)
    time.sleep(10)


def verify_result(args,verify_sql,verify_schema,verify_data):
    tc = unittest.TestCase()
    print(f"Executing sql: {verify_sql}")
    spark = get_spark(args)
    df = spark.sql(verify_sql).collect()
    for row in df:
        print(row)
    rows = verify_data.splitlines()
    tc.assertEqual(len(df), len(rows))
    for (row1, row2) in zip(df, rows):
        print(f"Row1: {row1}, Row 2: {row2}")
        row2 = row2.split(',')
        for idx, ty in enumerate(verify_schema):
            if ty == "int" or ty == "long":
                tc.assertEqual(row1[idx], int(row2[idx]))
            elif ty == "float" or ty == "double":
                tc.assertEqual(round(row1[idx], 5), round(float(row2[idx]), 5))
            elif ty == "boolean":
                tc.assertEqual(row1[idx], strtobool(row2[idx]))
            elif ty == "date":
                tc.assertEqual(row1[idx], strtodate(row2[idx]))
            elif ty == "timestamp":
                tc.assertEqual(row1[idx].astimezone(timezone.utc).replace(tzinfo=None), strtots(row2[idx]))
            elif ty == "timestamp_ntz":
                tc.assertEqual(row1[idx], datetime.fromisoformat(row2[idx]))
            elif ty == "string":
                tc.assertEqual(row1[idx], row2[idx])
            else:
                tc.fail(f"Unsupported type {ty}")

def drop_table(args,drop_sqls):
    spark = get_spark(args)
    for sql in drop_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test script for iceberg")
    parser.add_argument("-t", dest="test_case", type=str, help="Test case file")
    with open(parser.parse_args().test_case,"rb") as test_case:
        test_case = toml.load(test_case)
        # Extract content from testcase
        init_sqls = test_case['init_sqls']
        print(f"init_sqls:{init_sqls}")
        slt = test_case['slt']
        print(f"slt:{slt}")
        verify_schema = test_case['verify_schema']
        print(f"verify_schema:{verify_schema}")
        verify_sql = test_case['verify_sql']
        print(f"verify_sql:{verify_sql}")
        verify_data = test_case['verify_data']
        drop_sqls = test_case['drop_sqls']

        config = configparser.ConfigParser()
        config.read("config.ini")
        print({section: dict(config[section]) for section in config.sections()})

        init_iceberg_table(config,init_sqls)
        init_risingwave_mv(config,slt)
        verify_result(config,verify_sql,verify_schema,verify_data)
        drop_table(config,drop_sqls)
