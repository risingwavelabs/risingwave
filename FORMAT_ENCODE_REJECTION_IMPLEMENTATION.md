# Implementation: Reject FORMAT ENCODE Options in WITH Clause

This document describes the implementation to reject FORMAT ENCODE options when they are incorrectly placed in the WITH clause of CREATE SINK statements, as requested in issue #15824.

## Problem Statement

Previously, RisingWave would silently accept FORMAT ENCODE options in the WITH clause, but these options would not take effect. For example:

**Incorrect (previously accepted silently):**
```sql
CREATE SINK ...
WITH (
    ...
    timestamptz.handling.mode='utc_without_suffix'
) FORMAT UPSERT ENCODE JSON;
```

**Correct usage:**
```sql
CREATE SINK ...
WITH (
    ...
) FORMAT UPSERT ENCODE JSON (
    timestamptz.handling.mode='utc_without_suffix'
);
```

## Implementation Details

### Files Modified

1. **`src/frontend/src/handler/create_sink.rs`**
   - Added `FORMAT_ENCODE_OPTIONS` constant with list of known FORMAT ENCODE options
   - Added `check_format_encode_options_in_with_clause()` function
   - Integrated the validation into the sink creation pipeline

2. **`e2e_test/sink/kafka/reject_format_encode_in_with.slt`** (new file)
   - Comprehensive tests for rejecting various FORMAT ENCODE options in WITH clause
   - Tests for correct usage to ensure functionality is not broken

3. **`e2e_test/sink/kafka/create_sink.slt`**
   - Additional tests for the rejection functionality

### Key Implementation Components

#### 1. FORMAT ENCODE Options List
A comprehensive list of known FORMAT ENCODE options that should not appear in WITH clause:

```rust
static FORMAT_ENCODE_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    hashset! {
        // JSON encode options
        "timestamptz.handling.mode",
        "jsonb.handling.mode",
        // Protobuf/Avro schema options
        "schema.location", 
        "schema.registry",
        "message",
        "key.message",
        // AWS Glue schema registry
        "aws.glue.schema_arn",
        // Avro options
        "map.handling.mode",
        // Schema registry options
        "schema.registry.name.strategy",
    }
});
```

#### 2. Validation Function
```rust
fn check_format_encode_options_in_with_clause(
    resolved_with_options: &WithOptionsSecResolved,
) -> Result<()> {
    let (options, _) = resolved_with_options.clone().into_parts();
    
    for option_key in options.keys() {
        if FORMAT_ENCODE_OPTIONS.contains(option_key.as_str()) {
            return Err(RwError::from(ErrorCode::BindError(format!(
                "FORMAT ENCODE option '{}' should not be specified in the WITH clause. \
                 Please move it to the FORMAT ENCODE clause instead.\n\
                 \n\
                 Incorrect usage:\n\
                 CREATE SINK ... WITH (..., {}='...') FORMAT ... ENCODE ...;\n\
                 \n\
                 Correct usage:\n\
                 CREATE SINK ... WITH (...) FORMAT ... ENCODE ... ({}='...');",
                option_key, option_key, option_key
            ))));
        }
    }
    
    Ok(())
}
```

#### 3. Integration Point
The validation is called in `gen_sink_plan()` after the connector is determined but before processing the format description:

```rust
let connector = resolved_with_options
    .get(CONNECTOR_TYPE_KEY)
    .cloned()
    .ok_or_else(|| ErrorCode::BindError(format!("missing field '{CONNECTOR_TYPE_KEY}'")))?;

// Check for FORMAT ENCODE options in WITH clause and reject them
check_format_encode_options_in_with_clause(&resolved_with_options)?;

let format_desc = match stmt.sink_schema {
    // ... rest of format processing
};
```

## Error Messages

When a FORMAT ENCODE option is detected in the WITH clause, users will receive a clear error message:

```
FORMAT ENCODE option 'timestamptz.handling.mode' should not be specified in the WITH clause. Please move it to the FORMAT ENCODE clause instead.

Incorrect usage:
CREATE SINK ... WITH (..., timestamptz.handling.mode='...') FORMAT ... ENCODE ...;

Correct usage:
CREATE SINK ... WITH (...) FORMAT ... ENCODE ... (timestamptz.handling.mode='...');
```

## Testing

### Test Coverage
The implementation includes comprehensive tests for:

1. **Rejection of individual FORMAT ENCODE options:**
   - `timestamptz.handling.mode`
   - `jsonb.handling.mode`
   - `schema.location`
   - `schema.registry`
   - `message`
   - `key.message`
   - `aws.glue.schema_arn`
   - `map.handling.mode`
   - `schema.registry.name.strategy`

2. **Correct usage verification:**
   - Ensuring that FORMAT ENCODE options work correctly when placed in the proper FORMAT ENCODE clause
   - Verifying that normal WITH options (non-FORMAT ENCODE) continue to work

3. **SQL examples tested:**
   ```sql
   -- This should fail
   CREATE SINK test_reject_timestamptz FROM t WITH (
     connector = 'kafka',
     properties.bootstrap.server = 'localhost:9092',
     topic = 'test-topic',
     timestamptz.handling.mode = 'utc_without_suffix'
   ) FORMAT UPSERT ENCODE JSON;
   
   -- This should succeed
   CREATE SINK test_correct_usage FROM t WITH (
     connector = 'kafka',
     properties.bootstrap.server = 'localhost:9092',
     topic = 'test-topic'
   ) FORMAT UPSERT ENCODE JSON (
     timestamptz.handling.mode = 'utc_without_suffix'
   );
   ```

## Benefits

1. **Better User Experience:** Users get immediate feedback when they use incorrect syntax
2. **Prevents Silent Failures:** Options that don't work are rejected instead of being ignored
3. **Clear Guidance:** Error messages provide examples of correct usage
4. **Future-Proof:** Easy to add new FORMAT ENCODE options to the validation list

## Backward Compatibility

This change introduces a **breaking change** for any existing sinks that incorrectly specify FORMAT ENCODE options in the WITH clause. However, since these options were not working before, this change helps users fix their configurations rather than continue with non-functional setups.

## Location in Codebase

- **Main implementation:** `src/frontend/src/handler/create_sink.rs` lines 89-103 (constant) and 936-961 (function)
- **Integration point:** `src/frontend/src/handler/create_sink.rs` line 217 (validation call)
- **Tests:** `e2e_test/sink/kafka/reject_format_encode_in_with.slt`