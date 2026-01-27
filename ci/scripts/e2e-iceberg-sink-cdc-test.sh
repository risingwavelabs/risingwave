#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# CI environment: Connect to db service (PostgreSQL with CDC support)
export PGHOST="db"
export PGPORT="5432"
export PGUSER="postgres"
export PGPASSWORD='post\tgres'
export PGDATABASE="postgres"

echo "--- Testing Iceberg Sink with PostgreSQL CDC Schema Change"

verify_iceberg_source() {
    local source="$1"
    local expected
    local expected_file
    local actual_file
    local retry_count=0
    local max_retries=4

    shift
    expected=$(cat)

    # Create temporary files
    expected_file=$(mktemp)
    actual_file=$(mktemp)

    # Write expected results
    echo "$expected" > "$expected_file"

    while [ $retry_count -lt $max_retries ]; do
        # If not the first attempt, wait 2 seconds
        if [ $retry_count -gt 0 ]; then
            echo "⚠️  ${source} verification failed (attempt $retry_count/$max_retries), retrying in 2 seconds..."
            sleep 2
        fi

        # Get actual results (use -t to remove header, -A for unaligned format, space-separated)
        # Filter out cargo-make log lines, keep only query results
        risedev psql -t -A -F ' ' -c "SELECT * FROM ${source} ORDER BY id;" 2>&1 | grep -v '\[cargo-make\]' | grep -v '^$' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' > "$actual_file"

        # Check for ERROR (query execution failed)
        if grep -q "ERROR:" "$actual_file"; then
            retry_count=$((retry_count + 1))
            if [ $retry_count -ge $max_retries ]; then
                echo "❌ ${source} query failed after $max_retries attempts"
                echo ""
                echo "Query error:"
                cat "$actual_file"
                rm -f "$expected_file" "$actual_file"
                exit 1
            fi
            continue
        fi

        # Compare results
        if diff -u "$expected_file" "$actual_file" > /dev/null 2>&1; then
            echo "✅ ${source} matches expected results"
            rm -f "$expected_file" "$actual_file"
            return 0
        fi

        retry_count=$((retry_count + 1))
    done

    # All retries failed
    echo "❌ ${source} does NOT match expected results after $max_retries attempts"
    echo ""
    echo "Expected:"
    cat "$expected_file"
    echo ""
    echo "Actual:"
    cat "$actual_file"
    echo ""
    echo "Diff:"
    diff -u "$expected_file" "$actual_file" || true
    rm -f "$expected_file" "$actual_file"
    exit 1
}

# Create simple table with id and v1
psql -c "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (
        id INT PRIMARY KEY,
        v1 INT NOT NULL
    );
"

# Insert initial data
psql -c "INSERT INTO t (id, v1) VALUES (1, 10), (2, 20);"

# Create CDC source (connect to db service)
echo "Creating PostgreSQL CDC source..."
risedev psql -c "create source s with (
  username = 'postgres',
  connector='postgres-cdc',
  hostname='db',
  port='5432',
  password = 'post\\tgres',
  database.name = 'postgres',
  schema.name = 'public',
  slot.name = 'rw_cdc_test_slot',
  auto.schema.change = 'true'
);"

sleep 5

# Create CDC table in RisingWave
echo "Creating CDC table..."
risedev psql -c "create table t (
  id int primary key,
  v1 int
) from s table 'public.t';"

sleep 3

echo "Creating Iceberg sink with auto schema change..."
risedev psql -c "
CREATE SINK s1 from t WITH (
    connector = 'iceberg',
    type = 'append-only',
    database.name = 'public',
    table.name = 't',
    catalog.name = 'iceberg',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/iceberg-data',
    s3.endpoint = 'http://127.0.0.1:9301',
    s3.region = 'custom',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 2,
    primary_key = 'id',
    force_append_only='true',
    auto.schema.change = 'true',
    is_exactly_once = 'true',
);"


risedev psql -c "
CREATE SOURCE iceberg_s1
WITH (
    connector = 'iceberg',
    s3.endpoint = 'http://127.0.0.1:9301',
    s3.region = 'us-east-1',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/iceberg-data',
    database.name = 'public',
    table.name = 't',
);"

# Wait for data to be synced to Iceberg
echo "Waiting for data to sync to Iceberg..."
sleep 5

echo "Verifying initial data in iceberg_s1..."
risedev psql -c "select * from iceberg_s1 ORDER BY id;"
verify_iceberg_source iceberg_s1 <<'EOF'
1 10
2 20
EOF

# Insert more data before schema change
echo "Inserting more initial data..."
psql -c "INSERT INTO t (id, v1) VALUES (3, 30);"
sleep 3

echo "Performing schema change: ADD COLUMN v2 INT..."
psql -c "
    ALTER TABLE t ADD COLUMN v2 INT DEFAULT 0;
    INSERT INTO t (id, v1, v2) VALUES (4, 40, 100);
"
sleep 5

echo "Inserting more data with new column..."
psql -c "INSERT INTO t (id, v1, v2) VALUES (5, 50, 200);"
sleep 3



echo "Creating iceberg_s2 to verify schema change..."
risedev psql -c "
CREATE SOURCE iceberg_s2
WITH (
    connector = 'iceberg',
    s3.endpoint = 'http://127.0.0.1:9301',
    s3.region = 'us-east-1',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/iceberg-data',
    database.name = 'public',
    table.name = 't',
);"


echo "Waiting for data to sync to Iceberg..."
sleep 5

echo "Verifying data after first schema change..."
risedev psql -c "select * from iceberg_s2 ORDER BY id;"
verify_iceberg_source iceberg_s2 <<'EOF'
1 10
2 20
3 30
4 40 100
5 50 200
EOF

echo "Performing second schema change: ADD COLUMN v3 INT..."
psql -c "
    INSERT INTO t (id, v1, v2) VALUES (6, 60, 600);
    ALTER TABLE t ADD COLUMN v3 INT DEFAULT 10;
    INSERT INTO t (id, v1, v2, v3) VALUES (7, 70, 700, 700);
"
sleep 5

echo "Creating iceberg_s3 to verify second schema change..."
risedev psql -c "
CREATE SOURCE iceberg_s3
WITH (
    connector = 'iceberg',
    s3.endpoint = 'http://127.0.0.1:9301',
    s3.region = 'us-east-1',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/iceberg-data',
    database.name = 'public',
    table.name = 't',
);"
sleep 5

echo "Waiting for data to sync to Iceberg..."
sleep 5


echo "Verifying data after second schema change..."
risedev psql -c "select * from iceberg_s3 ORDER BY id";
verify_iceberg_source iceberg_s3 <<'EOF'
1 10
2 20
3 30
4 40 100
5 50 200
6 60 600
7 70 700 700
EOF

echo "--- Testing Iceberg Sink schema change with partition shuffle"

ICEBERG_PARTITION_DB_NAME="public"
ICEBERG_PARTITION_TABLE_NAME="t_partitioned"
RW_PARTITION_SOURCE_NAME="s_partitioned_cdc"
RW_PARTITION_TABLE_NAME="t_partitioned_upstream"

echo "Creating PostgreSQL CDC source for partitioned test (fresh backfill)..."
risedev psql -c "
    DROP SOURCE IF EXISTS ${RW_PARTITION_SOURCE_NAME};
    CREATE SOURCE ${RW_PARTITION_SOURCE_NAME} WITH (
        username = 'postgres',
        connector='postgres-cdc',
        hostname='db',
        port='5432',
        password = 'post\\tgres',
        database.name = 'postgres',
        schema.name = 'public',
        slot.name = 'rw_cdc_test_slot_partitioned',
        auto.schema.change = 'true'
    );
"

sleep 5

echo "Creating CDC table for partitioned test..."
risedev psql -c "
    DROP TABLE IF EXISTS ${RW_PARTITION_TABLE_NAME};
    CREATE TABLE ${RW_PARTITION_TABLE_NAME} (
        id int primary key,
        v1 int,
        v2 int,
        v3 int
    ) FROM ${RW_PARTITION_SOURCE_NAME} TABLE 'public.t';
"

sleep 5

echo "Preparing a partitioned Iceberg table..."
# NOTE: `StreamSink` will only add `StreamProject` + shuffle `StreamExchange` if the target Iceberg
# table exists and has sparse partitions (identity/bucket/truncate). So we create the table first
# with `partition_by` and without writing any data (id < 0).
risedev psql -c "
    DROP SINK IF EXISTS iceberg_partition_prepare;
    CREATE SINK iceberg_partition_prepare AS
    SELECT id, v1, v2, v3 FROM ${RW_PARTITION_TABLE_NAME} WHERE id < 0
    WITH (
        connector = 'iceberg',
        type = 'append-only',
        database.name = '${ICEBERG_PARTITION_DB_NAME}',
        table.name = '${ICEBERG_PARTITION_TABLE_NAME}',
        catalog.name = 'iceberg',
        catalog.type = 'storage',
        warehouse.path = 's3a://hummock001/iceberg-data',
        s3.endpoint = 'http://127.0.0.1:9301',
        s3.region = 'custom',
        s3.access.key = 'hummockadmin',
        s3.secret.key = 'hummockadmin',
        create_table_if_not_exists = 'true',
        partition_by = 'bucket(16,id)',
        force_append_only='true'
    );
"

sleep 5
risedev psql -c "DROP SINK iceberg_partition_prepare;"

echo "Creating Iceberg sink (partitioned table exists, should shuffle by partition)..."
risedev psql -c "
    DROP SINK IF EXISTS s_partitioned;
    CREATE SINK s_partitioned from ${RW_PARTITION_TABLE_NAME} WITH (
        connector = 'iceberg',
        type = 'append-only',
        database.name = '${ICEBERG_PARTITION_DB_NAME}',
        table.name = '${ICEBERG_PARTITION_TABLE_NAME}',
        catalog.name = 'iceberg',
        catalog.type = 'storage',
        warehouse.path = 's3a://hummock001/iceberg-data',
        s3.endpoint = 'http://127.0.0.1:9301',
        s3.region = 'custom',
        s3.access.key = 'hummockadmin',
        s3.secret.key = 'hummockadmin',
        commit_checkpoint_interval = 2,
        primary_key = 'id',
        force_append_only='true',
        auto.schema.change = 'true',
        is_exactly_once = 'true',
    );
"

echo "DESCRIBE FRAGMENTS s_partitioned (Iceberg partition shuffle)..."
risedev psql -c 'DESCRIBE FRAGMENTS s_partitioned;'

echo "Creating iceberg_p1 to verify initial backfill..."
risedev psql -c "
    DROP SOURCE IF EXISTS iceberg_p1;
    CREATE SOURCE iceberg_p1
    WITH (
        connector = 'iceberg',
        s3.endpoint = 'http://127.0.0.1:9301',
        s3.region = 'us-east-1',
        s3.access.key = 'hummockadmin',
        s3.secret.key = 'hummockadmin',
        s3.path.style.access = 'true',
        catalog.type = 'storage',
        warehouse.path = 's3a://hummock001/iceberg-data',
        database.name = '${ICEBERG_PARTITION_DB_NAME}',
        table.name = '${ICEBERG_PARTITION_TABLE_NAME}',
    );
"

echo "Waiting for data to sync to Iceberg..."
sleep 5

echo "Verifying initial data in iceberg_p1..."
risedev psql -c "select * from iceberg_p1 ORDER BY id;"
verify_iceberg_source iceberg_p1 <<'EOF'
1 10 0 10
2 20 0 10
3 30 0 10
4 40 100 10
5 50 200 10
6 60 600 10
7 70 700 700
EOF

echo "Performing third schema change: ADD COLUMN v4 INT..."
psql -c "
    INSERT INTO t (id, v1, v2, v3) VALUES (8, 80, 800, 800);
    ALTER TABLE t ADD COLUMN v4 INT DEFAULT 0;
    INSERT INTO t (id, v1, v2, v3, v4) VALUES (9, 90, 900, 900, 900);
"
sleep 5

echo "DESCRIBE FRAGMENTS s_partitioned (Iceberg partition shuffle)..."
risedev psql -c 'DESCRIBE FRAGMENTS s_partitioned;'

echo "Creating iceberg_p2 to verify schema change..."
risedev psql -c "
    DROP SOURCE IF EXISTS iceberg_p2;
    CREATE SOURCE iceberg_p2
    WITH (
        connector = 'iceberg',
        s3.endpoint = 'http://127.0.0.1:9301',
        s3.region = 'us-east-1',
        s3.access.key = 'hummockadmin',
        s3.secret.key = 'hummockadmin',
        s3.path.style.access = 'true',
        catalog.type = 'storage',
        warehouse.path = 's3a://hummock001/iceberg-data',
        database.name = '${ICEBERG_PARTITION_DB_NAME}',
        table.name = '${ICEBERG_PARTITION_TABLE_NAME}',
    );
"

echo "Waiting for data to sync to Iceberg..."
sleep 5

echo "Verifying data after third schema change..."
risedev psql -c "select * from iceberg_p2 ORDER BY id";
verify_iceberg_source iceberg_p2 <<'EOF'
1 10 0 10
2 20 0 10
3 30 0 10
4 40 100 10
5 50 200 10
6 60 600 10
7 70 700 700
8 80 800 800
9 90 900 900 900
EOF

echo "✅ PostgreSQL CDC with Iceberg sink schema change test completed successfully!"
