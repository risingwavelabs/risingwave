# Bug Fix: ALTER SOURCE ... REFRESH SCHEMA fails for sources with generated columns

## Problem Description

When using `ALTER SOURCE ... REFRESH SCHEMA` on a source that has generated columns, the operation fails with an error like:

```
db error: ERROR: Failed to run the query

Caused by:
  Feature is not yet implemented: this altering statement will drop columns, which is not supported yet: (t: integer)
```

This happens even though the generated column `t` is explicitly defined in the source definition and should be preserved.

## Root Cause Analysis

The issue is in the `columns_minus` function in `src/frontend/src/handler/alter_source_with_sr.rs` (lines 83-91).

During `ALTER SOURCE ... REFRESH SCHEMA`:

1. **Step 1**: The system fetches the current source columns (including generated columns like `t int as id+1`)
2. **Step 2**: The system resolves new columns from the schema registry (which only contains columns from the external schema, NOT generated columns)
3. **Step 3**: The `columns_minus` function calculates "dropped columns" by finding columns in the original source that are not in the newly resolved columns
4. **Step 4**: The function incorrectly identifies generated columns as "dropped" because they're not present in the schema registry columns

The original `columns_minus` function filtered out:
- Hidden columns (`!col_a.is_hidden()`)
- Connector additional columns (`!col_a.is_connector_additional_column()`)

But it **did not** filter out generated columns, leading to the bug.

## Solution

Added a filter to exclude generated columns when calculating dropped columns:

```rust
fn columns_minus(columns_a: &[ColumnCatalog], columns_b: &[ColumnCatalog]) -> Vec<ColumnCatalog> {
    columns_a
        .iter()
        .filter(|col_a| {
            !col_a.is_hidden()
                && !col_a.is_connector_additional_column()
                && !col_a.is_generated()  // <- NEW: Exclude generated columns
                && !columns_b.iter().any(|col_b| {
                    col_a.name() == col_b.name() && col_a.data_type() == col_b.data_type()
                })
        })
        .cloned()
        .collect()
}
```

## Why This Fix Is Correct

1. **Generated columns are defined in SQL**: They are part of the source definition (e.g., `t int as id+1`) and should be preserved during schema refresh
2. **Generated columns are not from external schema**: The schema registry only contains columns from the external system, not SQL-defined generated columns
3. **Consistency with existing logic**: The function already excludes other SQL-defined elements (hidden columns, additional columns)

## Files Changed

1. **`src/frontend/src/handler/alter_source_with_sr.rs`**:
   - Modified `columns_minus` function to exclude generated columns
   - Updated documentation to clarify the behavior

2. **`e2e_test/source_inline/kafka/protobuf/alter_source.slt`**:
   - Added test case for generated columns in existing ALTER SOURCE test file
   - Tests that generated columns are preserved during schema refresh
   - Tests that new schema registry columns are properly added

## Test Case

The test creates a source with a generated column and merges it into the existing `alter_source.slt` test file:

```sql
CREATE SOURCE src_user_gen (*, t int as id+1)
INCLUDE timestamp
WITH (...)
FORMAT PLAIN ENCODE PROTOBUF(...);
```

Then adds new fields to the schema registry and verifies that `ALTER SOURCE ... REFRESH SCHEMA` works correctly:
- Generated column `t` is preserved
- New column `age` from schema registry is added
- Generated column continues to work correctly

## Impact

This fix resolves the immediate issue where `ALTER SOURCE ... REFRESH SCHEMA` fails for sources with generated columns, allowing users to refresh their schema while keeping their SQL-defined generated columns intact.