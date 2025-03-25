from typing import Dict, Optional

from pyspark.sql import SparkSession

from .log import log, LogLevel


g_spark: Optional[SparkSession] = None


def get_spark(args) -> SparkSession:
    spark_config = args["spark"]
    global g_spark
    if g_spark is None:
        g_spark = SparkSession.builder.remote(spark_config["url"]).getOrCreate()

    return g_spark


def init_iceberg_table(args, init_sqls):
    log(f"init_sqls:", level=LogLevel.INFO)
    spark = get_spark(args)
    for sql in init_sqls:
        log(sql, level=LogLevel.INFO, indent=2)
        spark.sql(sql)


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
        for sql in drop_sqls:
            log(f"Executing sql: {sql}", level=LogLevel.INFO)
            spark.sql(sql)
    except Exception as e:
        log(f"drop table failed: {e}", level=LogLevel.ERROR)
        for db in spark.catalog.listDatabases():
            tables = spark.catalog.listTables(db.name)
            log(f"existing tables: {tables}", level=LogLevel.INFO)
        raise e
