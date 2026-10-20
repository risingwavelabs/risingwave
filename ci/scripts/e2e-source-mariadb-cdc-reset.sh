#!/usr/bin/env bash

# Verify persisted binlog offset recovery, then expired offsets and ALTER SOURCE RESET.
# For local runs, use a profile with a user-managed MariaDB that survives risedev kill:
# MARIADB_HOST=127.0.0.1 MARIADB_TCP_PORT=8307 MARIADB_RISEDEV_PROFILE=<profile> bash <this script>
set -euo pipefail

export MARIADB_HOST=${MARIADB_HOST:-mariadb}
export MARIADB_TCP_PORT=${MARIADB_TCP_PORT:-3306}
export MARIADB_PWD=${MARIADB_PWD:-123456}
mariadb_risedev_profile=${MARIADB_RISEDEV_PROFILE:-ci-source-cdc-test}

run_mariadb() {
  mysql --host="${MARIADB_HOST}" --port="${MARIADB_TCP_PORT}" \
    --user=root --password="${MARIADB_PWD}" "$@"
}

wait_for_count() {
  local sql=$1
  local expected=$2
  local success_marker="__RW_EXPECTED_COUNT__"
  local output
  for _ in {1..30}; do
    if output=$(risedev psql -tAc \
      "SELECT '${success_marker}' WHERE (${sql}) = ${expected}") \
      && grep -Fxq "${success_marker}" <<< "${output}"; then
      return 0
    fi
    sleep 1
  done
  risedev psql -c "${sql}"
  return 1
}

wait_for_binlog_reader() {
  for _ in {1..30}; do
    if [[ $(run_mariadb -sN -e \
      "SELECT count(*) FROM information_schema.PROCESSLIST WHERE COMMAND = 'Binlog Dump'") -gt 0 ]]; then
      return 0
    fi
    sleep 1
  done
  run_mariadb -e "SHOW PROCESSLIST;"
  return 1
}

echo "--- Run MariaDB CDC persisted binlog offset recovery test"
risedev kill
risedev clean-data
risedev ci-resume "${mariadb_risedev_profile}"

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

# Persist both the consumed binlog offset and downstream state before stopping RW.
risedev psql -c "FLUSH;"
risedev kill

# These changes must be read from the saved offset, even across a binlog rotation.
# Starting at the latest offset would lose these changes.
run_mariadb -e "
  USE mariadb_reset_test;
  START TRANSACTION;
  UPDATE test_table SET value = 'updated-offline' WHERE id = 1;
  DELETE FROM test_table WHERE id = 2;
  INSERT INTO test_table VALUES (4, 'inserted-offline');
  COMMIT;
  FLUSH LOGS;
  INSERT INTO test_table VALUES (5, 'rotated-offline');
"

risedev ci-resume "${mariadb_risedev_profile}"
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id = 1 AND value = 'updated-offline'" 1
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id = 2" 0
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id = 4 AND value = 'inserted-offline'" 1
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id = 5 AND value = 'rotated-offline'" 1
wait_for_count "SELECT count(*) FROM mariadb_reset_table" 3

# Check that streaming continues after catching up, and restore the RESET fixture.
run_mariadb -e "
  USE mariadb_reset_test;
  START TRANSACTION;
  UPDATE test_table SET value = 'before' WHERE id = 1;
  INSERT INTO test_table VALUES (2, 'before');
  DELETE FROM test_table WHERE id IN (4, 5);
  COMMIT;
"
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id IN (1, 2) AND value = 'before'" 2
wait_for_count "SELECT count(*) FROM mariadb_reset_table" 2
risedev psql -c "FLUSH;"

echo "--- Run MariaDB CDC binlog expiration and RESET test"
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
risedev ci-resume "${mariadb_risedev_profile}"
# RESET starts at the latest offset. Wait until it has chosen that offset before
# writing the row that must be consumed; frontend readiness alone is insufficient.
wait_for_binlog_reader
run_mariadb -e "INSERT INTO mariadb_reset_test.test_table VALUES (100, 'after-reset');"
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id = 100" 1
wait_for_count "SELECT count(*) FROM mariadb_reset_table" 3
wait_for_count "SELECT count(*) FROM mariadb_reset_table WHERE id IN (3, 50)" 0

risedev psql -c "DROP TABLE mariadb_reset_table; DROP SOURCE mariadb_reset_source CASCADE;"
run_mariadb -e "DROP DATABASE mariadb_reset_test;"

# Stop the cluster before the next CI stage cleans data and starts a new one.
risedev kill && risedev clean-data
