#!/bin/bash

# This script must be executed from `cargo make run-java-binding-demo`.

set -e

TABLE_NAME=java_binding_demo

psql -d dev -h localhost -p 4566 -U root << EOF
DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME} (v1 bigint, v2 varchar, v3 bigint);
INSERT INTO ${TABLE_NAME} values (1, 'aaa', 1), (2, 'bbb', 2);
FLUSH;
EOF

TABLES=$(./risedev ctl table list | grep \#)
DEMO_TABLE_PATTERN="#([0-9]+): ${TABLE_NAME}"

# Get table id for demo table.
if [[ ${TABLES} =~ ${DEMO_TABLE_PATTERN} ]]; then
    TABLE_ID=${BASH_REMATCH[1]}
else
    echo "Demo table ${TABLE_NAME} not found"
    exit 1
fi

# TODO: change hard-coded hummock url

cd ${JAVA_BINDING_ROOT}/java
TABLE_ID=${TABLE_ID} STATE_STORE=hummock+s3://zhidong-s3-bench mvn exec:exec \
    -pl java-binding \
    -Dexec.executable=java \
    -Dexec.args=" \
        -cp %classpath:java-binding/target*.jar:proto/target/*.jar \
        -Djava.library.path=${RISINGWAVE_ROOT}/target/debug com.risingwave.java.Demo"

psql -d dev -h localhost -p 4566 -U root << EOF
DROP TABLE ${TABLE_NAME};
EOF
