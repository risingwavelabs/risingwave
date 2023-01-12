#!/bin/bash

# This script must be executed from `cargo make run-java-binding-demo`.

set -e

TABLE_NAME=java_binding_demo
DB_NAME=dev
# State store URL and meta node address is determined by risedev.
STATE_STORE=hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001
META_ADDR=127.0.0.1:5690

${RISINGWAVE_ROOT}/risedev d java-binding-demo

psql -d ${DB_NAME} -h localhost -p 4566 -U root << EOF
DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME} (v1 bigint, v2 varchar, v3 bigint);
INSERT INTO ${TABLE_NAME} values (1, 'aaa', 1), (2, 'bbb', 2);
FLUSH;
EOF

cd ${JAVA_BINDING_ROOT}/java

TABLE_NAME=${TABLE_NAME} \
DB_NAME=${DB_NAME} \
STATE_STORE=${STATE_STORE} \
META_ADDR=${META_ADDR} \
mvn exec:exec \
    -pl java-binding \
    -Dexec.executable=java \
    -Dexec.args=" \
        -cp %classpath:java-binding/target*.jar:proto/target/*.jar \
        -Djava.library.path=${RISINGWAVE_ROOT}/target/debug com.risingwave.java.Demo"

psql -d dev -h localhost -p 4566 -U root << EOF
DROP TABLE ${TABLE_NAME};
EOF

cd -
${RISINGWAVE_ROOT}/risedev k > /dev/null
