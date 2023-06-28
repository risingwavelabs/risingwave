from pyspark.sql import SparkSession

def get_spark():
    spark = SparkSession.builder \
        .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .getOrCreate()
        # .config('spark.sql.debug.maxToStringFields', 2000) \
        # .config('spark.debug.maxToStringFields', 2000) \
        # .config('spark.driver.memory', '16g') \
        # .config('spark.executor.memory', '16g') \
        # .config('spark.driver.maxResultSize', '4g') \
        # .config('spark.network.timeout', 180) \
    spark.conf.set("spark.sql.catalog.demo",
                   "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.demo.type", "hadoop")
    spark.conf.set("spark.sql.catalog.demo.warehouse",
                   "s3a://iceberg/")
    spark.conf.set("spark.sql.catalog.demo.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9301") 
    spark.conf.set("spark.sql.catalog.demo.hadoop.fs.s3a.access.key", "hummockadmin")
    spark.conf.set("spark.sql.catalog.demo.hadoop.fs.s3a.secret.key", "hummockadmin")
    return spark

if __name__ == "__main__":
    spark = get_spark()
    spark_df = spark.sql("CALL demo.system.expire_snapshots('demo.demo_db.demo_table')").show()
    spark_df = spark.sql("CALL demo.system.remove_orphan_files('demo.demo_db.demo_table')").show()
    spark.stop()