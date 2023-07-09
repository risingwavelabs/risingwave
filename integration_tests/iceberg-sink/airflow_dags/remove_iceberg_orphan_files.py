import datetime
import pendulum

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


DAG_ID = "remove_iceberg_orphan_files"
PYSPARK_APPLICATION_PATH = "./iceberg-compaction-sql/remove_orphan_files.py"
SPARK_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.3.2"

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval= datetime.timedelta(days=1),
    tags=["iceberg"],
) as dag:

    spark_sql_remove_files = SparkSubmitOperator(
        application=PYSPARK_APPLICATION_PATH,
        task_id="spark_sql_remove_files", 
        packages=SPARK_PACKAGES,  
        conn_id = "spark_local",
    )