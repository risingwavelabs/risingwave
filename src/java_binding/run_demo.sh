#!/usr/bin/env bash

# This script must be executed from `cargo make run-java-binding-demo`.

set -ex

psql -d ${DB_NAME} -h localhost -p 4566 -U root << EOF
DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME} (v1 smallint, v2 int, v3 bigint, v4 float4, v5 float8, v6 bool, v7 varchar, may_null bigint);
INSERT INTO ${TABLE_NAME} values (1, 1, 1, 1.0, 1.0, false, 'aaa', 1), (2, 2, 2, 2.0, 2.0, true, 'bbb', NULL);
FLUSH;
EOF

cd ${RISINGWAVE_ROOT}/java

mvn exec:exec \
    -pl java-binding \
    -Dexec.executable=java \
    -Dexec.args=" \
        -cp %classpath:java-binding/target*.jar:proto/target/*.jar \
        -Djava.library.path=${RISINGWAVE_ROOT}/target/debug com.risingwave.java.Demo"

psql -d dev -h localhost -p 4566 -U root << EOF
DROP TABLE ${TABLE_NAME};
EOF