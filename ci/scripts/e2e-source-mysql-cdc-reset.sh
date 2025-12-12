#!/bin/bash

# Test: MySQL CDC binlog offset expiration and ALTER SOURCE RESET recovery
#
# This test verifies the behavior when MySQL binlog containing the CDC offset expires,
# and validates the ALTER SOURCE RESET command for recovery.
#
# Expected behavior:
# 1. ALTER SOURCE RESET sets the expired offset in state table to NULL
# 2. Requires one recovery (restart) to reset Debezium and update offset from NULL to latest available offset
# 3. After recovery, new data after the latest offset is consumed normally
# 4. Data between expired offset and latest offset is lost (expected data loss)
#
# Test scenario:
# - Batch 1 (id=1-5): Consumed before binlog expiration -> SHOULD BE PRESENT
# - Batch 2 (id=6-10): Written while source paused, binlog purged -> LOST (binlog expired)
# - Batch 3 (id=16-20): Written after RESET but before restart -> LOST (offset not persisted)
# - Batch 4 (id=101-120): Written after restart cluster -> SHOULD BE PRESENT
# - Expected final count: 25 rows (5 + 20)

set -euo pipefail

export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456

echo "\n\n\n-------------Run MySQL CDC binlog expire and RESET test------------\n\n\n"

# Cleanup
risedev kill && risedev clean-data

# Setup CDC table with initial schema
risedev ci-start
echo "\n\n\n-------------RW started------------\n\n\n"

# Create database and test table
echo "Creating test database and table..."
mysql -e "
    DROP DATABASE IF EXISTS binlog_test;
    CREATE DATABASE binlog_test;
    USE binlog_test;
    CREATE TABLE test_table (
        id INT PRIMARY KEY,
        value VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"

# Create CDC source
echo "Creating CDC source..."
risedev psql -c "CREATE SOURCE s WITH (
    username = '${MYSQL_USER:-root}',
    connector = 'mysql-cdc',
    hostname = '${MYSQL_HOST}',
    port = '${MYSQL_TCP_PORT}',
    password = '${MYSQL_PWD}',
    database.name = 'binlog_test'
);"

sleep 3

# Create CDC table
echo "Creating CDC table..."
risedev psql -c "CREATE TABLE test_table (
    id INT PRIMARY KEY,
    value VARCHAR,
    created_at TIMESTAMPTZ
) FROM s TABLE 'binlog_test.test_table';"

sleep 5

echo "\n\n\n-------------Phase 1: Insert first batch and let RW consume------------\n\n\n"
echo "Inserting first batch (id=1-5)..."
for i in {1..5}; do
    mysql -e "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch1_$i');"
done

sleep 5

echo "--- Verify first batch (should be 5 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 5 THEN 'OK' ELSE 'FAIL' END FROM test_table;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: First batch has 5 rows"
else
    echo "✗ FAIL: First batch does not have 5 rows"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table;"
    exit 1
fi

echo "\n\n\n-------------Phase 2: Pause source------------\n\n\n"
echo "Pausing source with rate_limit=0..."
risedev psql -c "ALTER SOURCE s SET source_rate_limit = 0;"

sleep 2

echo "\n\n\n-------------Phase 3: Insert second batch (will not be consumed)------------\n\n\n"
echo "Inserting second batch (id=6-10) while source is paused..."
for i in {6..10}; do
    mysql -e "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch2_$i');"
done

sleep 3

echo "--- Verify second batch not consumed (should still be 5 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 5 THEN 'OK' ELSE 'FAIL' END FROM test_table;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: Second batch not consumed, still 5 rows"
else
    echo "✗ FAIL: Count is not 5"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table;"
    exit 1
fi

echo "\n\n\n-------------Phase 4: Force binlog rotation and PURGE------------\n\n\n"
echo "Flushing logs to rotate binlog..."
mysql -e "FLUSH LOGS;"

sleep 2

echo "Inserting dummy data to new binlog..."
mysql -e "USE binlog_test; INSERT INTO test_table (id, value) VALUES (50, 'dummy_data');"

sleep 1

echo "Purging old binlog files..."
CURRENT_BINLOG=$(mysql -s -N -e "SHOW BINARY LOG STATUS;" 2>/dev/null | awk '{print $1}' || mysql -s -N -e "SHOW MASTER STATUS;" 2>/dev/null | awk '{print $1}')
mysql -e "PURGE BINARY LOGS TO '$CURRENT_BINLOG';"

sleep 2

echo "\n\n\n-------------Phase 5: Resume source (should fail to consume expired binlog)------------\n\n\n"
echo "Resuming source..."
risedev psql -c "ALTER SOURCE s SET source_rate_limit = default;"

sleep 5

echo "--- Verify source cannot consume expired binlog (should still be 5 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 5 THEN 'OK' ELSE 'FAIL' END FROM test_table;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: Source cannot consume expired binlog, still 5 rows"
else
    echo "✗ FAIL: Count is not 5, binlog may not be expired"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table;"
    exit 1
fi

sleep 2

echo "\n\n\n-------------Phase 6: Execute ALTER SOURCE RESET------------\n\n\n"
echo "Executing ALTER SOURCE RESET..."
risedev psql -c "ALTER SOURCE s RESET;"

sleep 2

echo "\n\n\n-------------Phase 7: Insert data after RESET------------\n\n\n"
echo "Inserting third batch (id=16-20) after RESET..."
for i in {16..20}; do
    mysql -e "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch3_after_reset_$i');"
done

sleep 5

echo "Restarting RW to ensure offset persistence..."
risedev kill
sleep 2
risedev ci-start
sleep 5

echo "Inserting fourth batch (id=101-120) after restart..."
for i in {101..120}; do
    mysql -e "USE binlog_test; INSERT INTO test_table (id, value) VALUES ($i, 'batch4_after_restart_$i');"
done

sleep 10

echo "\n\n\n-------------Final Verification------------\n\n\n"

FINAL_RW_COUNT=$(risedev psql -t -c "SELECT COUNT(*) FROM test_table;" 2>&1 | grep -E '^[0-9]+$' | head -n 1 | tr -d ' ')
FINAL_MYSQL_COUNT=$(mysql -s -N -e "USE binlog_test; SELECT COUNT(*) FROM test_table;" 2>/dev/null)

echo "MySQL total records: $FINAL_MYSQL_COUNT"
echo "RisingWave total records: $FINAL_RW_COUNT"

# Verify batch 1 (id=1-5): should have 5 rows
echo "--- Verify batch 1 (id=1-5, should be 5 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 5 THEN 'OK' ELSE 'FAIL' END FROM test_table WHERE id BETWEEN 1 AND 5;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: Batch 1 has 5 rows"
else
    echo "✗ FAIL: Batch 1 does not have 5 rows"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 1 AND 5;"
    exit 1
fi

# Verify batch 2 (id=6-10): should have 0 rows (binlog expired)
echo "--- Verify batch 2 (id=6-10, should be 0 rows - binlog expired)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FAIL' END FROM test_table WHERE id BETWEEN 6 AND 10;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: Batch 2 has 0 rows (binlog expired as expected)"
else
    echo "✗ FAIL: Batch 2 should have 0 rows"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 6 AND 10;"
    exit 1
fi

# Verify batch 3 (id=16-20): should have 0 rows (not persisted before restart)
echo "--- Verify batch 3 (id=16-20, should be 0 rows - not persisted before restart)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FAIL' END FROM test_table WHERE id BETWEEN 16 AND 20;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: Batch 3 has 0 rows (not persisted as expected)"
else
    echo "✗ FAIL: Batch 3 should have 0 rows"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 16 AND 20;"
    exit 1
fi

# Verify batch 4 (id=101-120): should have 20 rows
echo "--- Verify batch 4 (id=101-120, should be 20 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 20 THEN 'OK' ELSE 'FAIL' END FROM test_table WHERE id BETWEEN 101 AND 120;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: Batch 4 has 20 rows"
else
    echo "✗ FAIL: Batch 4 does not have 20 rows"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table WHERE id BETWEEN 101 AND 120;"
    exit 1
fi

# Verify total count (should be 25: batch1(5) + batch4(20))
echo "--- Verify total count (should be 25 rows)"
OUTPUT=$(risedev psql -t -c "SELECT CASE WHEN COUNT(*) = 25 THEN 'OK' ELSE 'FAIL' END FROM test_table;" 2>&1)
if echo "$OUTPUT" | grep -q "OK"; then
    echo "✓ PASS: Total count is 25 rows (batch1 + batch4)"
    echo "  - Lost data as expected: batch2(5, binlog expired) + batch3(5, not persisted) + dummy(1)"
else
    echo "✗ FAIL: Total count is not 25"
    echo "Debug output: $OUTPUT"
    risedev psql -c "SELECT COUNT(*) FROM test_table;"
    exit 1
fi

echo "\n\n\n-------------All verifications passed------------\n\n\n"

echo "\n\n\n-------------Cleanup test environment------------\n\n\n"

# Drop table and source
risedev psql -c "DROP TABLE test_table;"
risedev psql -c "DROP SOURCE s;"

# Drop database
mysql -e "DROP DATABASE IF EXISTS binlog_test;"

echo "\n\n\n-------------Cleanup completed------------\n\n\n"

# Cleanup
risedev kill && risedev clean-data
