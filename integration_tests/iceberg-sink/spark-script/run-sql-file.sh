set -ex

/opt/spark/bin/spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.1.0,org.apache.hadoop:hadoop-aws:3.3.2\
    --conf spark.jars.ivy=${HOME}/work-dir/.ivy2 \
    --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.demo.type=hadoop \
    --conf spark.sql.catalog.demo.warehouse=s3a://hummock001/iceberg-data \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://minio-0:9301 \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.path.style.access=true \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=hummockadmin \
    --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=hummockadmin \
    --conf spark.sql.defaultCatalog=demo \
    -f /spark-script/$1.sql