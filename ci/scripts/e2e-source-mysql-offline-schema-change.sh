#!/bin/bash

set -euo pipefail

export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456

echo "\n\n\n-------------Run mysql offline schema change test------------\n\n\n"

# Cleanup
risedev kill && risedev clean-data

# Setup CDC table with initial schema
risedev ci-resume mysql-offline-schema-change-test
echo "\n\n\n-------------RW started------------\n\n\n"

mysql -e "
    create database if not exists risedev;
    USE risedev;
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (k int primary key, v text);
"

mysql -e "
    create database if not exists test_db;
    USE test_db;
    DROP TABLE IF EXISTS t1;
    CREATE TABLE t1 (k int primary key, v text);
"

# sqllogictest -p 4566 -d dev reproduce-21801.slt
risedev psql -c "create source s with (
  username = '${MYSQL_USER:-root}',
  connector='mysql-cdc',
  hostname='${MYSQL_HOST}',
  port='${MYSQL_TCP_PORT}',
  password = '${MYSQL_PWD}',
  database.name = 'risedev',
  debezium.schema.history.internal.max.records.per.file = 1,
  auto.schema.change = 'true'
);
"

risedev psql -c "create source s1 with (
  username = '${MYSQL_USER:-root}',
  connector='mysql-cdc',
  hostname='${MYSQL_HOST}',
  port='${MYSQL_TCP_PORT}',
  password = '${MYSQL_PWD}',
  database.name = 'test_db',
  auto.schema.change = 'true'
);
"

sleep 5

risedev psql -c "create table t (
  k int primary key,
  v text
) from s table 'risedev.t';"

risedev psql -c "create table t1 (
  k int primary key,
  v text
) from s1 table 'test_db.t1';"

mysql -e "
    USE risedev;
    INSERT INTO t VALUES (1, 'abc');
"
mysql -e "
    USE test_db;
    INSERT INTO t1 VALUES (1, 'abc');
"

sleep 10
echo "--- Verify t before schema change (should be 1 row)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 1 THEN 'OK' ELSE 'FAIL' END FROM t;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: t has 1 row before schema change"
else
    echo "✗ FAIL: t does not have 1 row"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM t;"
    exit 1
fi

sleep 5
echo "--- Verify t1 before schema change (should be 1 row)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 1 THEN 'OK' ELSE 'FAIL' END FROM t1;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: t1 has 1 row before schema change"
else
    echo "✗ FAIL: t1 does not have 1 row"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM t1;"
    exit 1
fi


echo "\n\n\n-------------Take RW offline------------\n\n\n"
#
risedev kill

# Resume MySQL only, perform some writes, then schema change, then some more writes
risedev dev mysql-only



echo "\n\n\n-------------Change Schema------------\n\n\n"

# docker exec -it risedev-mysql-8306 mysql -u root -p -D risedev  "insert into t values (2, 'def');alter table t add column v2 int default 42;insert into t values (3, 'ghi', 88);"
mysql -u root -D risedev -e "insert into t values (2, 'def'); alter table t add column v2 int default 42; insert into t values (3, 'ghi', 88);"
mysql -u root -D test_db -e "insert into t1 values (2, 'def'); alter table t1 add column v2 int default 55; insert into t1 values (4, 'ghi', 88);"

risedev kill

echo "\n\n\n-------------Resume RW CDC------------\n\n\n"
sleep 5
# Resume RW CDC without cleaning data
risedev ci-resume mysql-offline-schema-change-test

# Verify data
# If the bug is reproduced, you won't see rows with k=2 and k=3, check the logs of compute-node!

sleep 20
echo "--- Verify t after schema change (should be 3 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 3 THEN 'OK' ELSE 'FAIL' END FROM t;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: t has 3 rows after schema change"
else
    echo "✗ FAIL: t does not have 3 rows"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM t;"
    exit 1
fi

echo "--- Verify t has 3 columns"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 3 THEN 'OK' ELSE 'FAIL' END FROM information_schema.columns WHERE table_name = 't';" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: t has 3 columns after schema change"
else
    echo "✗ FAIL: t does not have 3 columns"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT column_name FROM information_schema.columns WHERE table_name = 't';"
    exit 1
fi

sleep 10
echo "--- Verify t1 after schema change (should be 3 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 3 THEN 'OK' ELSE 'FAIL' END FROM t1;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: t1 has 3 rows after schema change"
else
    echo "✗ FAIL: t1 does not have 3 rows"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM t1;"
    exit 1
fi

echo "--- Verify t1 has 3 columns"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 3 THEN 'OK' ELSE 'FAIL' END FROM information_schema.columns WHERE table_name = 't1';" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: t1 has 3 columns after schema change"
else
    echo "✗ FAIL: t1 does not have 3 columns"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT column_name FROM information_schema.columns WHERE table_name = 't1';"
    exit 1
fi


echo "\n\n\n-------------All verifications passed------------\n\n\n"

echo "\n\n\n-------------Cleanup test environment------------\n\n\n"

# Drop tables first
risedev psql -c "drop table t;"
risedev psql -c "drop table t1;"

# Drop sources
risedev psql -c "drop source s;"
risedev psql -c "drop source s1;"

echo "\n\n\n-------------Cleanup completed------------\n\n\n"

# Cleanup
risedev kill && risedev clean-data

