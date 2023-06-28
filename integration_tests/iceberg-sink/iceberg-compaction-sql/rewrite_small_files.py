import time
from remove_orpan_files import get_spark

num_rewrite_manifest_retries = 3
if __name__ == "__main__":
    spark = get_spark()
    spark_df = spark.sql("CALL demo.system.rewrite_data_files('demo.demo_db.demo_table')").show()
    for i in range(num_rewrite_manifest_retries):
        time.sleep(2)
        try:
            spark.sql(f"CALL iceberg.system.rewrite_manifests('demo.demo_db.demo_table')").show()
        except Exception as exc:
            if i + 1 == num_rewrite_manifest_retries:
                raise exc
        break
    spark.stop()