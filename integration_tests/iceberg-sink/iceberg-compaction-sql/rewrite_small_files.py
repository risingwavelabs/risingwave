import time
from remove_orphan_files import get_spark

num_rewrite_manifest_retries = 3
if __name__ == "__main__":
    spark = get_spark()
    spark.sql("CALL demo.system.rewrite_data_files('demo.demo_db.demo_table')").show()
    # There may be a failure here because of Iceberg's optimistic lock, so add a manual retryy
    for i in range(num_rewrite_manifest_retries):
        time.sleep(2)
        try:
            spark.sql(f"CALL demo.system.rewrite_manifests('demo.demo_db.demo_table')").show()
        except Exception as exc:
            if i + 1 == num_rewrite_manifest_retries:
                raise exc
        break
    spark.stop()