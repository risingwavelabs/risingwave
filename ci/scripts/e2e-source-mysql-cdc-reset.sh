#!/bin/bash

# Test MySQL CDC inject-source-offsets + ALTER SOURCE RESET + forward-offset skip behavior.

set -euo pipefail

export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456

strip_logs() {
    sed 's/\r$//' | grep -v '\[cargo-make\]' | sed '/^[[:space:]]*$/d'
}

query_scalar() {
    local sql="$1"
    risedev psql -t -A -c "$sql" 2>&1 | strip_logs | tail -n 1
}

mysql_exec() {
    local sql="$1"
    mysql -e "$sql"
}

query_i_retry() {
    local sql="$1"
    local expected="$2"
    local retry="$3"
    local backoff="$4"
    local tmp
    tmp=$(mktemp)
    cat > "$tmp" <<SLT
query I retry ${retry} backoff ${backoff}
${sql}
----
${expected}
SLT
    risedev slt "$tmp"
    rm -f "$tmp"
}

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

echo "------------- baseline consume ------------"
for i in {1..5}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch1_$i');"
done
query_i_retry "SELECT COUNT(*) FROM test_table;" "5" 30 "1s"

echo "------------- inject validation ------------"
SOURCE_ID=$(query_scalar "SELECT id FROM rw_sources WHERE name = 's';")
STATE_TABLE=$(query_scalar "SELECT name FROM rw_catalog.rw_internal_table_info WHERE job_name = 's' AND job_type = 'source' LIMIT 1;")

[[ "$SOURCE_ID" =~ ^[0-9]+$ ]] || { echo "✗ FAIL: invalid SOURCE_ID=${SOURCE_ID}"; exit 1; }
[[ "$STATE_TABLE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || { echo "✗ FAIL: invalid STATE_TABLE=${STATE_TABLE}"; exit 1; }

STATE_ROW=$(query_scalar "SELECT partition_id || '|' || (offset_info->'split_info'->'inner'->>'start_offset') FROM ${STATE_TABLE} WHERE offset_info->'split_info'->'inner'->>'start_offset' IS NOT NULL LIMIT 1;")
[ -n "$STATE_ROW" ] || { echo "✗ FAIL: missing non-null start_offset row"; exit 1; }

SPLIT_ID="${STATE_ROW%%|*}"
CURRENT_OFFSET="${STATE_ROW#*|}"
OFFSETS_JSON=$(python3 -c 'import json,sys; print(json.dumps({sys.argv[1]: sys.argv[2]}))' "$SPLIT_ID" "$CURRENT_OFFSET")
./risedev ctl meta inject-source-offsets --source-id "$SOURCE_ID" --offsets "$OFFSETS_JSON"

OFFSET_AFTER_INJECT=$(query_scalar "SELECT offset_info->'split_info'->'inner'->>'start_offset' FROM ${STATE_TABLE} WHERE partition_id='${SPLIT_ID}' LIMIT 1;")
if [ "$OFFSET_AFTER_INJECT" != "$CURRENT_OFFSET" ]; then
    echo "✗ FAIL: inject-source-offsets changed split state unexpectedly"
    echo "Before: $CURRENT_OFFSET"
    echo "After:  $OFFSET_AFTER_INJECT"
    exit 1
fi
echo "✓ PASS: inject-source-offsets keeps split state consistent"

echo "------------- forward offset skip validation ------------"
risedev psql -c "ALTER SOURCE s SET source_rate_limit = 0;"

for i in {9001..9003}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'inject_skip_$i');"
done

BINLOG_STATUS=$(mysql -s -N -e "SHOW BINARY LOG STATUS;" 2>/dev/null || mysql -s -N -e "SHOW MASTER STATUS;" 2>/dev/null)
BINLOG_FILE=$(echo "$BINLOG_STATUS" | awk '{print $1}')
BINLOG_POS=$(echo "$BINLOG_STATUS" | awk '{print $2}')
[ -n "$BINLOG_FILE" ] && [ -n "$BINLOG_POS" ] || { echo "✗ FAIL: failed to get binlog file/pos"; exit 1; }

FORWARD_OFFSET=$(python3 -c 'import json,sys; o=json.loads(sys.argv[1]); so=o.setdefault("sourceOffset",{}); so["file"]=sys.argv[2]; so["pos"]=int(sys.argv[3]); so["snapshot"]=False; o["isHeartbeat"]=False; print(json.dumps(o,separators=(",",":")))' "$CURRENT_OFFSET" "$BINLOG_FILE" "$BINLOG_POS")
FORWARD_OFFSETS_JSON=$(python3 -c 'import json,sys; print(json.dumps({sys.argv[1]: sys.argv[2]}))' "$SPLIT_ID" "$FORWARD_OFFSET")
./risedev ctl meta inject-source-offsets --source-id "$SOURCE_ID" --offsets "$FORWARD_OFFSETS_JSON"

risedev psql -c "ALTER SOURCE s SET source_rate_limit = default;"
query_i_retry "SELECT COUNT(*) FROM test_table;" "5" 30 "1s"
query_i_retry "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 9001 AND 9003;" "0" 10 "1s"

echo "------------- reset validation ------------"
risedev psql -c "ALTER SOURCE s SET source_rate_limit = 0;"
for i in {6..10}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch2_$i');"
done
query_i_retry "SELECT COUNT(*) FROM test_table;" "5" 10 "1s"

mysql_exec "FLUSH LOGS;"
mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES (50, 'dummy_data');"
CURRENT_BINLOG=$(mysql -s -N -e "SHOW BINARY LOG STATUS;" 2>/dev/null | awk '{print $1}' || mysql -s -N -e "SHOW MASTER STATUS;" 2>/dev/null | awk '{print $1}')
mysql_exec "PURGE BINARY LOGS TO '$CURRENT_BINLOG';"

risedev psql -c "ALTER SOURCE s SET source_rate_limit = default;"
query_i_retry "SELECT COUNT(*) FROM test_table;" "5" 10 "1s"

risedev psql -c "ALTER SOURCE s RESET;"
query_i_retry "SELECT CASE WHEN COUNT(*) >= 1 THEN 1 ELSE 0 END FROM ${STATE_TABLE} WHERE offset_info->'split_info'->'inner'->>'start_offset' IS NULL;" "1" 10 "1s"

echo "------------- resume behavior validation ------------"
for i in {16..20}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch3_after_reset_$i');"
done

risedev kill
risedev ci-resume mysql-offline-schema-change-test

for i in {101..120}; do
    mysql_exec "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch4_after_restart_$i');"
done

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
