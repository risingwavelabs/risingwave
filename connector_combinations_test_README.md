# E2E Test for Sink and Table Connector Combinations (Issue #21798)

## Overview

This test covers all possible combinations of sinks (with/without connector) and tables (with/without connector) as requested in issue #21798.

## Test File Location

`e2e_test/sink/sink_into_table/connector_combinations.slt`

## Test Coverage

The test covers the following scenarios:

### 1. Sink Into Table (without connector) from Table (without connector) into Table (without connector)
- Tests basic sink into table functionality
- Verifies INSERT, UPDATE, DELETE operations are properly propagated
- Uses regular tables (no external connectors)

### 2. Sink Into Table (without connector) from Table (with connector) into Table (without connector)
- Tests sinking from a source table (with datagen connector) to a regular table
- Verifies that streaming data from external sources can be sunk into internal tables
- Uses datagen connector for reliable test data generation

### 3. Sink (with connector) from Table (without connector)
- Tests external sinks from regular tables
- Uses blackhole connector for reliable testing without external dependencies
- Verifies upsert operations work correctly

### 4. Sink (with connector) from Table (with connector)
- Tests external sinks from source tables
- Combines streaming source with external sink
- Uses datagen → blackhole connector chain for testing

### 5. Additional Edge Cases
- Tests append-only sinks into tables without primary keys
- Verifies append-only behavior (updates create new rows)
- Tests various table configurations

### 6. Error Cases and Boundary Conditions
- Tests invalid sink connector type 'table' (should fail)
- Tests attempting to sink into a source table (should fail)  
- Validates proper error handling

## Key Features Tested

- **Data Propagation**: Ensures data flows correctly from source to sink
- **Update Semantics**: Verifies upsert vs append-only behavior
- **Schema Compatibility**: Tests that data types are handled correctly
- **Error Handling**: Validates that invalid configurations fail gracefully
- **Connector Integration**: Tests integration between different connector types

## Test Execution

To run this test:

```bash
./risedev slt 'e2e_test/sink/sink_into_table/connector_combinations.slt'
```

## Notes

- Uses deterministic datagen configurations for reproducible results
- Includes appropriate sleep statements for async operations
- Uses blackhole connector to avoid external dependencies
- Tests both streaming and batch data patterns
- Covers both primary key and append-only table configurations

## Contribution to RisingWave

This test ensures that:
1. All valid sink→table combinations work correctly
2. Invalid combinations fail with appropriate error messages
3. Data consistency is maintained across different connector types
4. Performance and reliability are maintained across different scenarios

The test helps prevent regressions in sink and table functionality and ensures that new features don't break existing connector combinations.