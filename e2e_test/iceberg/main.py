from pyspark.sql import SparkSession
import configparser
import subprocess
import csv
import unittest

spark = None


def init_iceberg_table(args):
    spark_config = args['spark']
    global spark
    spark = SparkSession.builder.remote(spark_config['url']).getOrCreate()

    # init_table_sqls = [
    #     "CREATE SCHEMA IF NOT EXISTS demo_db",
    #     "DROP TABLE IF EXISTS demo_db.demo_table",
    #     """
    #     CREATE TABLE demo_db.demo_table (v1 int, v2 bigint, v3 string) TBLPROPERTIES ('format-version'='2');
    #     """,
    # ]
    #
    # for sql in init_table_sqls:
    #     print(f"Executing sql: {sql}")
    #     spark.sql(sql)


def init_risingwave_mv(args):
    rw_config = args['risingwave']
    cmd = f"sqllogictest -p {rw_config['port']} -d {rw_config['db']} iceberg_sink_v2.slt"
    print(f"Command line is [{cmd}]")
    subprocess.run(cmd,
                   shell=True,
                   check=True)


def verify_result(args):
    sql = "SELECT * FROM demo_db.demo_table ORDER BY v1,v2 ASC"
    tc = unittest.TestCase()
    print(f"Executing sql: {sql}")
    global spark
    df = spark.sql(sql).collect()
    for row in df:
        print(row)

    with open(args['default']['result'], newline='') as csv_file:
        csv_result = csv.reader(csv_file)
        for (row1, row2) in zip(df, csv_result):
            print(f"Row1: {row1}, row 2: {row2}")
            tc.assertEqual(row1[0], int(row2[0]))
            tc.assertEqual(row1[1], int(row2[1]))
            tc.assertEqual(row1[2], row2[2])


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    init_iceberg_table(config)
    # init_risingwave_mv(config)
    verify_result(config)
