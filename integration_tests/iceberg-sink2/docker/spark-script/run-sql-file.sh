set -ex

ICEBERG_VERSION=1.3.1
DEPENDENCIES="org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:$ICEBERG_VERSION,org.apache.hadoop:hadoop-aws:3.3.2"

## add AWS dependency
#AWS_SDK_VERSION=2.20.18
#AWS_MAVEN_GROUP=software.amazon.awssdk
#AWS_PACKAGES=(
#    "bundle"
#)
#for pkg in "${AWS_PACKAGES[@]}"; do
#    DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
#done

spark-sql --packages $DEPENDENCIES \
  --master local[3] \
  --files /spark-script/log4j.properties \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=hadoop \
  --conf spark.sql.catalog.demo.warehouse=s3a://icebergdata/demo \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.path.style.access=true \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=admin \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=password \
  --conf spark.sql.defaultCatalog=demo \
  -f /spark-script/$1.sql
