import time
from pyspark.sql import SparkSession

def get_spark():
    spark = SparkSession.builder \
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo","org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type","hadoop") \
        .config("spark.sql.catalog.demo.warehouse","s3a://hummock001/iceberg-data") \
        .config("spark.sql.catalog.demo.hadoop.fs.s3a.endpoint","http://minio-0:9301") \
        .config("spark.sql.catalog.demo.hadoop.fs.s3a.path.style.access","true") \
        .config("spark.sql.catalog.demo.hadoop.fs.s3a.access.key","hummockadmin") \
        .config("spark.sql.catalog.demo.hadoop.fs.s3a.secret.key","hummockadmin") \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark = get_spark()
    spark.sql(f"CALL demo.system.expire_snapshots('demo.demo_db.demo_table')").show()
    time.sleep(2)
    spark.sql(f"CALL demo.system.remove_orphan_files('demo.demo_db.demo_table')").show()
    spark.stop()