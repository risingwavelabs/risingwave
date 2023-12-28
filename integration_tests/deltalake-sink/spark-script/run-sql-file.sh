set -ex

/opt/spark/bin/spark-sql --packages io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.2\
    --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' \
    --conf 'spark.hadoop.fs.s3a.access.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.secret.key=hummockadmin' \
    --conf 'spark.hadoop.fs.s3a.endpoint=http://minio-0:9301' \
    --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
    -f /spark-script/$1.sql