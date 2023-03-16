#!/usr/bin/env bash

# This script must be executed from `cargo make run-java-binding-demo`.

set -ex


set +x
INSERT_DATA=$(python3 ${RISINGWAVE_ROOT}/src/java_binding/gen-demo-insert-data.py 30000)

psql -d ${DB_NAME} -h localhost -p 4566 -U root << EOF
DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME} (v1 smallint, v2 int, v3 bigint, v4 float4, v5 float8, v6 bool, v7 varchar, may_null bigint);
INSERT INTO ${TABLE_NAME} values ${INSERT_DATA};
FLUSH;
EOF

set -x

cd ${RISINGWAVE_ROOT}/java

java -cp "./java-binding-integration-test/target/dependency/*:./java-binding-integration-test/target/classes" \
    -Djava.library.path=${RISINGWAVE_ROOT}/target/debug  com.risingwave.java.binding.HummockReadDemo

psql -d dev -h localhost -p 4566 -U root << EOF
DROP TABLE ${TABLE_NAME};
EOF