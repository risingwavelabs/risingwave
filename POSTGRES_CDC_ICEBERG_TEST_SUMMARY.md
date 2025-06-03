# Postgres CDC with Iceberg Table Engine Test Case

## Summary

Added a comprehensive test case for Postgres CDC integration with Iceberg table engine, demonstrating the use of `auto.schema.change = 'true'` and the `ENGINE = iceberg` syntax.

## Files Created

### Test Configuration
- **`e2e_test/iceberg/test_case/cdc/postgres_cdc.toml`** - Test configuration file that defines the test environment, verification schema, and expected results

### Test Scripts  
- **`e2e_test/iceberg/test_case/cdc/postgres_cdc_load.slt`** - Main SQL Logic Test file containing the test flow
- **`e2e_test/iceberg/test_case/cdc/postgres_cdc.sql`** - Initial Postgres setup script that creates the schema and test data
- **`e2e_test/iceberg/test_case/cdc/postgres_cdc_insert.sql`** - Additional data insertion script for testing incremental updates

### Documentation
- **`e2e_test/iceberg/test_case/cdc/README.md`** - Comprehensive documentation explaining both the existing MySQL CDC test and the new Postgres CDC test

## Test Case Features

The new test demonstrates:

### 1. Postgres CDC Source with Auto Schema Change
```sql
create source ingestion.api_db_ingestion_auto_schema with (
    connector = 'postgres-cdc',
    hostname = '${PGHOST:localhost}',
    port = '${PGPORT:5432}',
    username = '${PGUSER:$USER}',
    password = '${PGPASSWORD:}',
    database.name = '${PGDATABASE:postgres}',
    schema.name = 'ingestion',
    auto.schema.change = 'true',  -- Key feature
    slot.name = 'ingestion_cdc_slot'
);
```

### 2. Traditional Table Creation from CDC Source
```sql
create table transactions ( 
    id bigint,
    merchant_id int,
    amount numeric,
    created_at timestamp,
    last_updated_at timestamp,
    new_col int,
    PRIMARY KEY (id)
) FROM ingestion.api_db_ingestion_auto_schema TABLE 'ingestion.transactions';
```

### 3. Iceberg Table Engine (New Feature)
```sql
create table ingestion.pg_transactions_auto_internal_stg ( 
    id bigint,
    merchant_id int,
    amount numeric,
    created_at timestamp,
    last_updated_at timestamp,
    new_col int,
    PRIMARY KEY (id)
)
with (commit_checkpoint_interval = 1)
FROM ingestion.api_db_ingestion_auto_schema TABLE 'ingestion.transactions'
ENGINE = iceberg;  -- Key feature
```

### 4. Real-world Transaction Schema
The test uses a realistic financial transactions table:
- `id` (bigserial) - Auto-incrementing primary key
- `merchant_id` (int4) - Foreign key to merchant
- `amount` (numeric(12,2)) - Precise financial amount
- `created_at` (timestamp) - Record creation time
- `last_updated_at` (timestamp) - Last modification time  
- `new_col` (int4) - Additional nullable column

## Test Flow

1. **Environment Setup**: Creates Postgres schema and populates initial data (8 transactions)
2. **Source Creation**: Creates shared CDC source with auto schema change enabled
3. **Table Creation**: Creates both traditional table and Iceberg engine table from the same source
4. **Sink Creation**: Creates traditional Iceberg sink for comparison
5. **Initial Verification**: Verifies both approaches capture the initial 8 records
6. **Incremental Data**: Inserts 5 additional transactions
7. **Final Verification**: Confirms both approaches capture all 13 transactions
8. **Data Validation**: Verifies data consistency and ordering
9. **Cleanup**: Properly tears down all resources

## Integration

The test is automatically discovered by the existing test runner at `e2e_test/iceberg/main.py` and will run as part of the CDC test suite in CI environments.

## Usage

To run the specific test:
```bash
cd e2e_test/iceberg
python main.py -t test_case/cdc/postgres_cdc.toml
```

To run all iceberg tests (including this one):
```bash
cd e2e_test/iceberg  
python main.py
```

## Benefits

This test case validates:
- ✅ Postgres CDC integration with Iceberg table engine
- ✅ Auto schema change functionality
- ✅ Shared CDC source usage pattern
- ✅ Data consistency between sink and table engine approaches  
- ✅ Incremental data capture
- ✅ Real-world business schema patterns
- ✅ Proper resource cleanup