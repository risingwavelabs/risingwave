#!/usr/bin/env bash

# Test MySQL CDC binlog expiration and ALTER SOURCE RESET recovery.
#
# This script intentionally focuses on RESET behavior only.
# inject-source-offsets skip/rewind behavior is covered by:
# e2e_test/source_inline/kafka/alter/cron_only/alter_source_properties_safe.slt.serial
#
# Scenario:
# 1. Consume batch1 (id=1..5).
# 2. Pause source and write batch2 (id=6..10), then purge old binlog.
# 3. Resume source; expired binlog data should not be consumed.
# 4. Execute ALTER SOURCE RESET.
# 5. Write batch3 (id=16..20), restart RW, then write batch4 (id=101..120).
# 6. Verify RESET recovery and expected data loss boundaries.

set -euo pipefail

export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456

query_i_retry() {
    local sql="$1"
    local expected="$2"
    local retry="$3"
    local backoff="$4"
    local tmp
    tmp=$(mktemp)
    cat >"$tmp" <<SLT
query I retry ${retry} backoff ${backoff}
${sql}
----
${expected}
SLT
    risedev slt "$tmp"
    rm -f "$tmp"
}

mysql_exec() {
    local sql="$1"
    mysql -e "$sql"
}

echo "------------- reset-only mysql cdc test ------------"
echo "inject-source-offsets skip behavior is validated in cron_only tests"

echo "------------- setup ------------"
risedev kill || true
risedev clean-data
risedev ci-resume mysql-offline-schema-change-test

mysql_exec "
DROP DATABASE IF EXISTS binlog_test;
CREATE DATABASE binlog_test;
USE binlog_test;
CREATE TABLE test_table (
  id INT PRIMARY KEY,
  value VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"

risedev psql -c "CREATE SOURCE s WITH (
  username = '${MYSQL_USER:-root}',
  connector = 'mysql-cdc',
  hostname = '${MYSQL_HOST}',
  port = '${MYSQL_TCP_PORT}',
  password = '${MYSQL_PWD}',
  database.name = 'binlog_test'
);"

risedev psql -c "CREATE TABLE test_table (
  id INT PRIMARY KEY,
  value VARCHAR,
  created_at TIMESTAMPTZ
) FROM s TABLE 'binlog_test.test_table';"

echo "------------- phase1: baseline consume ------------"
for i in {1..5}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch1_$i');"
done
query_i_retry "SELECT COUNT(*) FROM test_table;" "5" 30 "1s"

echo "------------- phase2: pause and write batch2 ------------"
risedev psql -c "ALTER SOURCE s SET source_rate_limit = 0;"
for i in {6..10}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch2_$i');"
done
query_i_retry "SELECT COUNT(*) FROM test_table;" "5" 10 "1s"

echo "------------- phase3: expire binlog and resume ------------"
mysql_exec "FLUSH LOGS;"
mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES (50, 'dummy_data');"
CURRENT_BINLOG=$(
    mysql -s -N -e "SHOW BINARY LOG STATUS;" 2>/dev/null | awk '{print $1}' ||
        mysql -s -N -e "SHOW MASTER STATUS;" 2>/dev/null | awk '{print $1}'
)
mysql_exec "PURGE BINARY LOGS TO '$CURRENT_BINLOG';"

risedev psql -c "ALTER SOURCE s SET source_rate_limit = default;"
query_i_retry "SELECT COUNT(*) FROM test_table;" "5" 10 "1s"

echo "------------- phase4: reset and restart ------------"
risedev psql -c "ALTER SOURCE s RESET;"

for i in {16..20}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch3_after_reset_$i');"
done

risedev kill
risedev ci-resume mysql-offline-schema-change-test

for i in {101..120}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch4_after_restart_$i');"
done

echo "------------- verify reset behavior ------------"
query_i_retry "SELECT COUNT(*) FROM test_table;" "25" 60 "1s"
query_i_retry "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 1 AND 5;" "5" 10 "1s"
query_i_retry "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 6 AND 10;" "0" 10 "1s"
query_i_retry "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 16 AND 20;" "0" 10 "1s"
query_i_retry "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 101 AND 120;" "20" 10 "1s"

echo "------------- cleanup ------------"
risedev psql -c "DROP TABLE test_table;"
risedev psql -c "DROP SOURCE s;"
mysql_exec "DROP DATABASE IF EXISTS binlog_test;"
risedev kill || true
risedev clean-data

echo "All verifications passed."
