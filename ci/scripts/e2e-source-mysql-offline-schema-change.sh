#!/bin/bash

set -euo pipefail

export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456

echo "\n\n\n-------------Run mysql offline schema change test------------\n\n\n"

# Cleanup
risedev k && risedev clean-data

# Setup CDC table with initial schema
risedev ci-start mysql-offline-schema-change-test
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
  database.name = 'test_db'
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

sleep 5
risedev psql -c "select * from t;"

sleep 5
risedev psql -c "select * from t1;"


echo "\n\n\n-------------Take RW offline------------\n\n\n"
#
risedev k

# Resume MySQL only, perform some writes, then schema change, then some more writes
risedev dev mysql-only

# docker exec -it risedev-mysql-8306 mysql -u root -p -D risedev  "USE risedev; DROP TABLE IF EXISTS t; CREATE TABLE t (k int primary key, v text);"

echo "\n\n\n-------------Change Schema------------\n\n\n"

# docker exec -it risedev-mysql-8306 mysql -u root -p -D risedev  "insert into t values (2, 'def');alter table t add column v2 int default 42;insert into t values (3, 'ghi', 88);"
mysql -u root -D risedev -e "insert into t values (2, 'def'); alter table t add column v2 int default 42; insert into t values (3, 'ghi', 88);"
mysql -u root -D test_db -e "insert into t1 values (2, 'def'); alter table t1 add column v2 int default 55; insert into t1 values (4, 'ghi', 88);"


echo "\n\n\n-------------Resume RW CDC------------\n\n\n"
sleep 5
# Resume RW CDC
risedev ci-start mysql-offline-schema-change-test

# Verify data
# If the bug is reproduced, you won't see rows with k=2 and k=3, check the logs of compute-node!

sleep 10
risedev psql -c "select * from t1;"

sleep 10
risedev psql -c "select * from t;"