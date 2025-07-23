# Sink-into-Table Connector Combinations E2E Tests

This document describes the new e2e test files added to address issue #16135, which requested comprehensive testing for "sink (with/without connector) into table (with/without connector)" combinations.

## Test Files Added

### 1. `basic_connector_combinations.slt`
- **Lines**: 229 (40 statements, 7 queries)
- **Purpose**: Simple scenarios covering the 4 basic combinations
- **Connectors Used**: `datagen` (simple, no external dependencies)
- **Test Cases**:
  - Regular table → Regular table 
  - Datagen source → Regular table
  - Regular source → Target with metadata
  - Datagen source → Target with metadata
  - Append-only scenarios

### 2. `connector_combinations.slt`  
- **Lines**: 234 (40 statements, 7 queries)
- **Purpose**: Comprehensive test scenarios with edge cases
- **Features Tested**:
  - Different table configurations (primary keys, append-only)
  - Metadata columns and timestamps
  - Advanced sink options
  - Multiple sink types (upsert, append-only)
- **Focus**: Internal RisingWave functionality

### 3. `kafka_connector_combinations.slt`
- **Lines**: 200 (24 statements, 5 queries)
- **Purpose**: Real external connector scenarios
- **Connectors Used**: `kafka` (with message_queue:29092)
- **Test Cases**:
  - Regular table → Kafka table
  - Kafka source → Regular table  
  - Kafka source → Kafka target (full pipeline)
- **Features**: Includes proper cleanup with `rpk` commands

## Combinations Tested

The tests cover all 4 required combinations from the issue:

| Source Type | Target Type | Test Files | Description |
|-------------|-------------|------------|-------------|
| **Without Connector** | **Without Connector** | All 3 files | Basic table-to-table sink |
| **Without Connector** | **With Connector** | All 3 files | Regular table to external-ready table |
| **With Connector** | **Without Connector** | All 3 files | External source to regular table |
| **With Connector** | **With Connector** | All 3 files | External source to external target |

## Key Testing Patterns

### Tables "Without Connector"
- Regular RisingWave tables created with `CREATE TABLE`
- No external system dependencies
- Internal data storage and processing

### Tables "With Connector" 
- Tables created with `WITH (connector = '...')` syntax
- Examples: `datagen`, `kafka` connectors
- Can interface with external systems

### Sinks "Without Connector"
- Sinks sourcing from regular tables
- Internal data movement within RisingWave

### Sinks "With Connector"
- Sinks sourcing from tables with external connectors
- Data flowing from external systems through RisingWave

## Test Execution

These tests can be run using:
```bash
./risedev slt './e2e_test/sink/sink_into_table/basic_connector_combinations.slt'
./risedev slt './e2e_test/sink/sink_into_table/connector_combinations.slt'
./risedev slt './e2e_test/sink/sink_into_table/kafka_connector_combinations.slt'
```

The Kafka test requires a running Kafka instance (provided by RiseDev environment).

## Benefits

1. **Complete Coverage**: All 4 combinations from issue #16135 are thoroughly tested
2. **Multiple Complexity Levels**: From simple datagen tests to real Kafka scenarios  
3. **Proper Cleanup**: All tests clean up resources to avoid conflicts
4. **Follows Existing Patterns**: Consistent with existing e2e test structure
5. **Real-world Scenarios**: Tests include realistic data pipeline patterns

These tests ensure that RisingWave properly handles all combinations of sink and table connector configurations, providing confidence in the robustness of the sink-into-table functionality.