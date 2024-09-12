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
import decimal;


def strtobool(v):
    return v.lower() == "true"


def strtodate(v):
    return date.fromisoformat(v)


def strtots(v):
    return datetime.fromisoformat(v).astimezone(timezone.utc).replace(tzinfo=None)


g_spark = None


def get_spark(args):
    spark_config = args["spark"]
    global g_spark
    if g_spark is None:
        g_spark = SparkSession.builder.remote(spark_config["url"]).getOrCreate()

    return g_spark


def init_iceberg_table(args, init_sqls):
    spark = get_spark(args)
    for sql in init_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)


def execute_slt(args, slt):
    if slt is None or slt == "":
        return
    rw_config = args["risingwave"]
    cmd = f"sqllogictest -p {rw_config['port']} -d {rw_config['db']} {slt}"
    print(f"Command line is [{cmd}]")
    subprocess.run(cmd, shell=True, check=True)
    time.sleep(15)


def verify_result(args, verify_sql, verify_schema, verify_data):
    tc = unittest.TestCase()

    time.sleep(3)
    print(f"verify_result:\nExecuting sql: {verify_sql}")
    spark = get_spark(args)
    df = spark.sql(verify_sql).collect()
    print(f"Result:")
    print(f"================")
    for row in df:
        print(row)
    print(f"================")
    rows = verify_data.splitlines()
    tc.assertEqual(len(df), len(rows), "row length mismatch")
    tc.assertEqual(len(verify_schema), len(df[0]), "column length mismatch")
    for row1, row2 in zip(df, rows):
        print(f"Row1: {row1}, Row 2: {row2}")
        # New parsing logic for row2
        row2 = parse_row(row2)
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
                tc.assertEqual(
                    row1[idx].astimezone(timezone.utc).replace(tzinfo=None),
                    strtots(row2[idx]),
                )
            elif ty == "timestamp_ntz":
                tc.assertEqual(row1[idx], datetime.fromisoformat(row2[idx]))
            elif ty == "string":
                tc.assertEqual(row1[idx], row2[idx])
            elif ty == "decimal":
                if row2[idx] == "none":
                    tc.assertTrue(row1[idx] is None)
                else:
                    tc.assertEqual(row1[idx], decimal.Decimal(row2[idx]))
            else:
                tc.assertEqual(str(row1[idx]), str(row2[idx]))

def compare_sql(args, cmp_sqls):
    assert len(cmp_sqls) == 2
    spark = get_spark(args)
    df1 = spark.sql(cmp_sqls[0])
    df2 = spark.sql(cmp_sqls[1])

    tc = unittest.TestCase()
    diff_df = df1.exceptAll(df2).collect()
    print(f"diff {diff_df}")
    tc.assertEqual(len(diff_df),0)
    diff_df = df2.exceptAll(df1).collect()
    print(f"diff {diff_df}")
    tc.assertEqual(len(diff_df),0)


def drop_table(args, drop_sqls):
    spark = get_spark(args)
    for sql in drop_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)


def parse_row(row):
    result = []
    current = ""
    parenthesis_count = {"{": 0, "[": 0, "(": 0}
    for char in row:
        if char in parenthesis_count:
            parenthesis_count[char] += 1
        elif char == "}":
            parenthesis_count["{"] -= 1
        elif char == "]":
            parenthesis_count["["] -= 1
        elif char == ")":
            parenthesis_count["("] -= 1

        if char == "," and all(value == 0 for value in parenthesis_count.values()):
            result.append(current.strip())
            current = ""
        else:
            current += char

    if current:
        result.append(current.strip())

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test script for iceberg")
    parser.add_argument("-t", dest="test_case", type=str, help="Test case file")

    with open(parser.parse_args().test_case, "rb") as test_case:
        test_case = toml.load(test_case)
        # Extract content from testcase
        init_sqls = test_case["init_sqls"]
        print(f"init_sqls:{init_sqls}")
        slt = test_case.get("slt")
        print(f"slt:{slt}")
        verify_schema = test_case.get("verify_schema")
        print(f"verify_schema:{verify_schema}")
        verify_sql = test_case.get("verify_sql")
        print(f"verify_sql:{verify_sql}")
        verify_data = test_case.get("verify_data")
        verify_slt = test_case.get("verify_slt")
        cmp_sqls = test_case.get("cmp_sqls")
        drop_sqls = test_case["drop_sqls"]
        config = configparser.ConfigParser()
        config.read("config.ini")
        print({section: dict(config[section]) for section in config.sections()})

        init_iceberg_table(config, init_sqls)
        if slt is not None and slt != "":
            execute_slt(config, slt)
        if (
            (verify_data is not None and verify_data != "")
            and (verify_sql is not None and verify_sql != "")
            and (verify_schema is not None and verify_schema != "")
        ):
            verify_result(config, verify_sql, verify_schema, verify_data)
        if cmp_sqls is not None and cmp_sqls != "" and len(cmp_sqls) == 2:
            compare_sql(config, cmp_sqls)
        if verify_slt is not None and verify_slt != "":
            execute_slt(config, verify_slt)
        if drop_sqls is not None and drop_sqls != "":
            drop_table(config, drop_sqls)
