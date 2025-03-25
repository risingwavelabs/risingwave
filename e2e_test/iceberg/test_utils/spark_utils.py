from typing import Dict, Optional
import unittest

from pyspark.sql import SparkSession

from .log import log, LogLevel


g_spark: Optional[SparkSession] = None


def get_spark(args) -> SparkSession:
    spark_config = args["spark"]
    global g_spark
    if g_spark is None:
        log(f"creating spark session with config: {spark_config}", level=LogLevel.DEBUG)
        g_spark = SparkSession.builder.remote(spark_config["url"]).getOrCreate()
        log(f"spark session created", level=LogLevel.DEBUG)
    else:
        log(f"use existing spark session", level=LogLevel.DEBUG)
    return g_spark


def run_sqls(spark, sqls):
    for sql in sqls:
        log(f"Executing Spark SQL: {sql}", level=LogLevel.INFO)
        spark.sql(sql)


def init_iceberg_table(args, init_sqls):
    log(f"init_sqls:", level=LogLevel.INFO)
    spark = get_spark(args)
    run_sqls(spark, init_sqls)


def compare_sql(args, cmp_sqls):
    assert len(cmp_sqls) == 2
    spark = get_spark(args)
    df1 = spark.sql(cmp_sqls[0])
    df2 = spark.sql(cmp_sqls[1])

    tc = unittest.TestCase()
    diff_df = df1.exceptAll(df2).collect()
    log(f"diff {diff_df}", level=LogLevel.INFO)
    tc.assertEqual(len(diff_df), 0)
    diff_df = df2.exceptAll(df1).collect()
    log(f"diff {diff_df}", level=LogLevel.INFO)
    tc.assertEqual(len(diff_df), 0)


def drop_table(args, drop_sqls):
    spark = get_spark(args)
    try:
        run_sqls(spark, drop_sqls)
    except Exception as e:
        log(f"drop table failed: {e}", level=LogLevel.ERROR)
        for db in spark.catalog.listDatabases():
            tables = spark.catalog.listTables(db.name)
            log(f"existing tables: {tables}", level=LogLevel.INFO)
        raise e
