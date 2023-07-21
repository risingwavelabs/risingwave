#!/usr/bin/env bash

# This script must be executed from `cargo make run-java-binding-demo`.

set -ex


set +x
INSERT_SQL=$(python3 ${RISINGWAVE_ROOT}/src/java_binding/gen-demo-insert-data.py 30000 ${TABLE_NAME})

echo ${INSERT_SQL}

psql -d ${DB_NAME} -h localhost -p 4566 -U root << EOF
DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME} (v1 smallint, v2 int, v3 bigint, v4 float4, v5 float8, v6 bool, v7 varchar, v8 timestamp, v9 decimal, may_null bigint);
${INSERT_SQL}
FLUSH;
EOF

set -x

cd ${RISINGWAVE_ROOT}/java

java -cp "./java-binding-integration-test/target/dependency/*:./java-binding-integration-test/target/classes" \
    com.risingwave.java.binding.HummockReadDemo

psql -d dev -h localhost -p 4566 -U root << EOF
DROP TABLE ${TABLE_NAME};
EOF