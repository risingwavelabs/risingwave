# Test parquet schema validation

## This example demonstrates the parquet schema validation functionality

### Example SQL that would now provide better error messages:

```sql
-- Create a source with user-defined columns that mismatch parquet schema
CREATE SOURCE s1 (
  id VARCHAR,              -- Parquet file has INT32 for this column
  name INT,                -- Parquet file has STRING for this column  
  age FLOAT
) WITH (
  connector = 's3',
  s3.region_name = 'us-west-2',
  s3.bucket_name = 'my-bucket',
  s3.credentials.access = 'access_key',
  s3.credentials.secret = 'secret_key',
  match_pattern = '*.parquet'
) FORMAT PLAIN ENCODE PARQUET;
```

### Before this change:
- The CREATE SOURCE would succeed
- Schema mismatches would be silently handled at runtime by filling with nulls
- Users would get confusing results without clear error messages

### After this change:
- The CREATE SOURCE will fail with a clear error message:
```
Data type mismatch for column "id". Defined in SQL as "VARCHAR", but found in the source as "INT32"
```

### Correct usage:
```sql
-- Let RisingWave automatically infer the schema from parquet files
CREATE SOURCE s1 WITH (
  connector = 's3',
  s3.region_name = 'us-west-2', 
  s3.bucket_name = 'my-bucket',
  s3.credentials.access = 'access_key',
  s3.credentials.secret = 'secret_key',
  match_pattern = '*.parquet'
) FORMAT PLAIN ENCODE PARQUET;

-- Or define columns that match the parquet schema
CREATE SOURCE s1 (
  id INT,                  -- Matches INT32 in parquet
  name VARCHAR,            -- Matches STRING in parquet
  age FLOAT                -- Matches DOUBLE in parquet
) WITH (
  connector = 's3',
  s3.region_name = 'us-west-2',
  s3.bucket_name = 'my-bucket', 
  s3.credentials.access = 'access_key',
  s3.credentials.secret = 'secret_key',
  match_pattern = '*.parquet'
) FORMAT PLAIN ENCODE PARQUET;
```