#!/usr/bin/env bash

# Verify recovery from an expired MariaDB binlog offset with ALTER SOURCE RESET.
set -euo pipefail

export MARIADB_HOST=mariadb MARIADB_TCP_PORT=3306 MARIADB_PWD=123456

run_mariadb() {
  mysql --host="${MARIADB_HOST}" --port="${MARIADB_TCP_PORT}" \
    --user=root --password="${MARIADB_PWD}" "$@"
}

wait_for_count() {
  local sql=$1
  local expected=$2
  for _ in {1..30}; do
    if [[ $(risedev psql -tAc "${sql}") == "${expected}" ]]; then
      return 0
    fi
    sleep 1
  done
  risedev psql -c "${sql}"
  return 1
}

echo "--- Run MariaDB CDC binlog expiration and RESET test"
risedev kill
risedev clean-data
risedev ci-resume ci-source-cdc-test

run_mariadb -e "
  DROP DATABASE IF EXISTS mariadb_reset_test;
  CREATE DATABASE mariadb_reset_test;
  USE mariadb_reset_test;
  CREATE TABLE test_table (id INT PRIMARY KEY, value VARCHAR(100));
"

risedev psql -c "CREATE SOURCE mariadb_reset_source WITH (
  connector = 'mariadb-cdc',
  hostname = '${MARIADB_HOST}',
  port = '${MARIADB_TCP_PORT}',
  username = 'root',
  password = '${MARIADB_PWD}',
  database.name = 'mariadb_reset_test'
);"
risedev psql -c "CREATE TABLE mariadb_reset_table (
  id INT PRIMARY KEY,
  value VARCHAR
) FROM mariadb_reset_source TABLE 'mariadb_reset_test.test_table';"

run_mariadb -e "INSERT INTO mariadb_reset_test.test_table VALUES (1, 'before'), (2, 'before');"
wait_for_count "SELECT count(*) FROM mariadb_reset_table" 2

risedev psql -c "ALTER SOURCE mariadb_reset_source SET source_rate_limit = 0;"
run_mariadb -e "INSERT INTO mariadb_reset_test.test_table VALUES (3, 'expired'); FLUSH LOGS;"
run_mariadb -e "INSERT INTO mariadb_reset_test.test_table VALUES (50, 'rotation');"
current_binlog=$(run_mariadb -sN -e "SHOW MASTER STATUS" | awk '{print $1}')
run_mariadb -e "PURGE BINARY LOGS TO '${current_binlog}';"
risedev psql -c "ALTER SOURCE mariadb_reset_source SET source_rate_limit = default;"

# The expired checkpoint cannot advance until it is explicitly reset.
sleep 5
wait_for_count "SELECT count(*) FROM mariadb_reset_table" 2
risedev psql -c "ALTER SOURCE mariadb_reset_source RESET;"

risedev kill
risedev ci-resume ci-source-cdc-test
run_mariadb -e "INSERT INTO mariadb_reset_test.test_table VALUES (100, 'after-reset');"
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id = 100" 1
wait_for_count "SELECT count(*) FROM mariadb_reset_table" 3
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id IN (3, 50)" 0

risedev psql -c "DROP TABLE mariadb_reset_table; DROP SOURCE mariadb_reset_source CASCADE;"
run_mariadb -e "DROP DATABASE mariadb_reset_test;"
