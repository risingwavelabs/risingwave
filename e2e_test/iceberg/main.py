from pyspark.sql import SparkSession
import configparser
import subprocess
import csv
import unittest
import time
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

init_table_sqls = [
    "CREATE SCHEMA IF NOT EXISTS demo_db",
    "DROP TABLE IF EXISTS demo_db.demo_table",
    """
    CREATE TABLE demo_db.demo_table (
    id long,
    v_int int,
    v_long long,
    v_float float,
    v_double double,
    v_varchar string,
    v_bool boolean,
    v_date date,
    v_timestamp timestamp,
    v_ts_ntz timestamp_ntz
    ) TBLPROPERTIES ('format-version'='2');
    """,
]


def get_spark(args):
    spark_config = args['spark']
    global g_spark
    if g_spark is None:
        g_spark = SparkSession.builder.remote(spark_config['url']).getOrCreate()

    return g_spark


def init_iceberg_table(args):
    spark = get_spark(args)
    for sql in init_table_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)


def init_risingwave_mv(args):
    rw_config = args['risingwave']
    cmd = f"sqllogictest -p {rw_config['port']} -d {rw_config['db']} iceberg_sink_v2.slt"
    print(f"Command line is [{cmd}]")
    subprocess.run(cmd,
                   shell=True,
                   check=True)
    time.sleep(60)


def verify_result(args):
    sql = "SELECT * FROM demo_db.demo_table ORDER BY id ASC"
    tc = unittest.TestCase()
    print(f"Executing sql: {sql}")
    spark = get_spark(args)
    df = spark.sql(sql).collect()
    for row in df:
        print(row)

    with open(args['default']['result'], newline='') as csv_file:
        csv_result = list(csv.reader(csv_file))
        for (row1, row2) in zip(df, csv_result):
            print(f"Row1: {row1}, row 2: {row2}")
            tc.assertEqual(row1[0], int(row2[0]))
            tc.assertEqual(row1[1], int(row2[1]))
            tc.assertEqual(row1[2], int(row2[2]))
            tc.assertEqual(round(row1[3], 5), round(float(row2[3]), 5))
            tc.assertEqual(round(row1[4], 5), round(float(row2[4]), 5))
            tc.assertEqual(row1[5], row2[5])
            tc.assertEqual(row1[6], strtobool(row2[6]))
            tc.assertEqual(row1[7], strtodate(row2[7]))
            tc.assertEqual(row1[8].astimezone(timezone.utc).replace(tzinfo=None), strtots(row2[8]))
            tc.assertEqual(row1[9], datetime.fromisoformat(row2[9]))

        tc.assertEqual(len(df), len(csv_result))


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    init_iceberg_table(config)
    init_risingwave_mv(config)
    verify_result(config)
