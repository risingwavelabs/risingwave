# CDC with Iceberg Table Engine Tests

This directory contains tests for Change Data Capture (CDC) integration with Iceberg table engine.

## Test Cases

### MySQL CDC with Iceberg (`load.slt`, `no_partition_cdc.toml`)
Tests the integration of MySQL CDC sources with Iceberg table engine, demonstrating:
- Basic CDC source creation with MySQL
- Creating both regular tables and Iceberg engine tables from the same CDC source
- Data verification through both sink and table engine approaches

### Postgres CDC with Iceberg (`postgres_cdc_load.slt`, `postgres_cdc.toml`)
Tests the integration of Postgres CDC sources with Iceberg table engine, demonstrating:
- **Auto Schema Change**: Uses `auto.schema.change = 'true'` to automatically detect schema changes
- **Shared CDC Source**: Creates a shared CDC source that can be used by multiple tables
- **Iceberg Table Engine**: Creates tables with `ENGINE = iceberg` that automatically persist to Iceberg storage
- **Real-world Schema**: Uses a transactions table schema similar to typical business applications

#### Key Features Tested:
1. **Postgres CDC Source Creation** with auto schema change enabled
2. **Regular Table Creation** from CDC source for comparison
3. **Iceberg Sink** creation from regular table (traditional approach)
4. **Iceberg Table Engine** creation directly from CDC source (new approach)
5. **Data Ingestion**: Verifies initial data load
6. **Incremental Updates**: Tests that new data is captured and stored
7. **Data Consistency**: Verifies data integrity across different storage methods

#### Schema Used:
The test uses a `transactions` table that simulates a financial transactions system:
```sql
CREATE TABLE ingestion.transactions (
    id bigserial NOT NULL,
    merchant_id int4 NOT NULL,
    amount numeric(12, 2) NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at timestamp NULL,
    new_col int4 NULL,
    CONSTRAINT transactions_pkey PRIMARY KEY (id)
);
```

This schema demonstrates:
- Serial primary key (common in business applications)
- Numeric precision for financial data
- Timestamp fields for audit trails
- Nullable columns for flexibility
- Multiple data types (bigint, int, numeric, timestamp)

#### Test Flow:
1. Setup Postgres database with test schema and initial data
2. Create Postgres CDC source with auto schema change
3. Create regular table from CDC source
4. Create Iceberg sink from regular table
5. Create Iceberg table engine directly from CDC source
6. Verify initial data count matches between approaches
7. Insert additional data into Postgres
8. Verify incremental data is captured in both approaches
9. Validate final data consistency
10. Clean up resources

This test validates that the Iceberg table engine works correctly with Postgres CDC and auto schema change, providing an alternative to the traditional sink approach for persisting CDC data to Iceberg.