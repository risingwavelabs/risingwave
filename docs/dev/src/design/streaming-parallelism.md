# Streaming Parallelism Configuration

This document provides comprehensive documentation for all streaming parallelism configuration variables in RisingWave.

## Overview

RisingWave provides fine-grained control over the parallelism of streaming jobs through session configuration variables. These configurations allow you to control:
1. **Initial parallelism**: How many parallel instances of a streaming job to create
2. **Adaptive strategy**: How the system should adjust parallelism when cluster size changes

## Configuration Hierarchy

The parallelism configuration follows a hierarchical structure:

```
streaming_parallelism (global default)
└── streaming_parallelism_for_<type> (type-specific override)

streaming_parallelism_strategy (global default strategy)
└── streaming_parallelism_strategy_for_<type> (type-specific strategy override)
```

Where `<type>` can be:
- `table` - Regular tables
- `materialized_view` - Materialized views
- `index` - Indexes
- `source` - Sources
- `sink` - Sinks
- `backfill` - Backfill operations

## Configuration Variables

### Initial Parallelism Variables

These variables control the initial number of parallel instances for streaming jobs.

#### `streaming_parallelism`

**Type**: `ConfigParallelism`  
**Default**: `default` (equivalent to `adaptive`)  
**Description**: Global default for all streaming jobs.

**Possible values**:
- `default` or `adaptive` or `0`: Adaptive parallelism based on cluster size
- Positive integer (e.g., `4`, `8`): Fixed parallelism

**Example**:
```sql
-- Set global streaming parallelism to 8
SET streaming_parallelism = 8;

-- Use adaptive parallelism (cluster size determines parallelism)
SET streaming_parallelism = 'adaptive';
```

#### Type-Specific Parallelism Variables

- `streaming_parallelism_for_table`
- `streaming_parallelism_for_materialized_view`
- `streaming_parallelism_for_index`
- `streaming_parallelism_for_source`
- `streaming_parallelism_for_sink`
- `streaming_parallelism_for_backfill`

**Type**: `ConfigParallelism`  
**Default**: `default` (falls back to `streaming_parallelism`)  

**Example**:
```sql
-- Set source parallelism to 4
SET streaming_parallelism_for_source = 4;

-- Set table parallelism to adaptive
SET streaming_parallelism_for_table = 'adaptive';

-- Use global default for sinks
SET streaming_parallelism_for_sink = 'default';
```

### Adaptive Parallelism Strategy Variables

These variables control how the system adjusts parallelism when the cluster size changes or when auto-scaling is triggered.

#### `streaming_parallelism_strategy`

**Type**: `ConfigAdaptiveParallelismStrategy`  
**Default**: `default` (follows system setting)  
**Description**: Global default adaptive strategy for all streaming jobs.

**Possible values**:
- `default`: Use the system-wide default strategy
- `AUTO`: Automatically use all available parallelism
- `FULL`: Always use full cluster parallelism
- `BOUNDED(n)`: Cap parallelism at `n` (e.g., `BOUNDED(8)`)
- `RATIO(r)`: Use a ratio of cluster parallelism (e.g., `RATIO(0.5)` for 50%)

**Example**:
```sql
-- Use automatic strategy (system default)
SET streaming_parallelism_strategy = 'AUTO';

-- Cap parallelism at 16
SET streaming_parallelism_strategy = 'BOUNDED(16)';

-- Use 50% of available parallelism
SET streaming_parallelism_strategy = 'RATIO(0.5)';
```

#### Type-Specific Strategy Variables

- `streaming_parallelism_strategy_for_table` (default: `BOUNDED(4)`)
- `streaming_parallelism_strategy_for_materialized_view` (default: `default`)
- `streaming_parallelism_strategy_for_index` (default: `default`)
- `streaming_parallelism_strategy_for_source` (default: `BOUNDED(4)`)
- `streaming_parallelism_strategy_for_sink` (default: `default`)

**Type**: `ConfigAdaptiveParallelismStrategy`  
**Description**: Type-specific adaptive parallelism strategies. Those with `default` fall back to `streaming_parallelism_strategy`.

**Note**: As of the latest version, both `source` and `table` default to `BOUNDED(4)` to prevent overwhelming external systems and limit initial resource usage.

**Example**:
```sql
-- Cap source parallelism at 4
SET streaming_parallelism_strategy_for_source = 'BOUNDED(4)';

-- Use 30% of cluster for materialized views
SET streaming_parallelism_strategy_for_materialized_view = 'RATIO(0.3)';

-- Use full parallelism for indexes
SET streaming_parallelism_strategy_for_index = 'FULL';
```

## Adaptive Strategy Behavior

### AUTO Strategy

Uses all available parallelism in the cluster.

**Example**:
- Cluster has 32 cores → parallelism = 32
- Cluster scales to 64 cores → parallelism = 64

### FULL Strategy

Identical to AUTO - uses all available parallelism.

### BOUNDED(n) Strategy

Caps parallelism at a maximum of `n`.

**Example**:
```sql
SET streaming_parallelism_strategy = 'BOUNDED(8)';
```

**Behavior**:
- Cluster has 4 cores → parallelism = 4
- Cluster has 8 cores → parallelism = 8
- Cluster has 16 cores → parallelism = 8 (capped)

### RATIO(r) Strategy

Uses a ratio `r` (between 0.0 and 1.0) of available parallelism, rounded down.

**Example**:
```sql
SET streaming_parallelism_strategy = 'RATIO(0.5)';
```

**Behavior**:
- Cluster has 4 cores → parallelism = 2 (0.5 * 4)
- Cluster has 10 cores → parallelism = 5 (0.5 * 10)
- Cluster has 7 cores → parallelism = 3 (0.5 * 7 = 3.5, rounded down)

## Configuration Priority

The system determines parallelism using the following priority:

1. **Type-specific parallelism** (`streaming_parallelism_for_<type>`)
   - If set to a fixed value: use that value
   - If set to `adaptive`: use adaptive parallelism with type-specific strategy
   - If set to `default` or not set: fall back to global setting

2. **Global parallelism** (`streaming_parallelism`)
   - If set to a fixed value: use that value
   - If set to `default` or `adaptive`: use adaptive parallelism

3. **Adaptive strategy** (when using adaptive parallelism)
   - Type-specific strategy if set (`streaming_parallelism_strategy_for_<type>`)
   - Otherwise, global strategy (`streaming_parallelism_strategy`)

## Common Use Cases

### Use Case 1: Fixed Parallelism for All Jobs

```sql
-- Set all streaming jobs to use 8 parallel instances
SET streaming_parallelism = 8;
```

### Use Case 2: Adaptive with Bounded Growth

```sql
-- Use adaptive parallelism but cap at 16
SET streaming_parallelism = 'adaptive';
SET streaming_parallelism_strategy = 'BOUNDED(16)';
```

### Use Case 3: Different Strategies per Job Type

```sql
-- Sources and tables: default to BOUNDED(4) (no need to set explicitly)
-- This prevents overwhelming external systems and limits resource usage

-- MVs: use 50% of cluster (balance between performance and resource usage)
SET streaming_parallelism_strategy_for_materialized_view = 'RATIO(0.5)';

-- Sinks: use full parallelism (maximize throughput)
SET streaming_parallelism_strategy_for_sink = 'FULL';
```

### Use Case 4: Override for Specific Table

```sql
-- Set a fixed parallelism for tables
SET streaming_parallelism_for_table = 4;

-- This table will now use 4 parallel instances
CREATE TABLE my_table (...);
```

### Use Case 5: Development vs Production

**Development**:
```sql
-- Use minimal parallelism to save resources
SET streaming_parallelism = 1;
```

**Production**:
```sql
-- Use adaptive parallelism
SET streaming_parallelism = 'adaptive';

-- Sources and tables already default to BOUNDED(4)
-- Optionally customize materialized view strategy
SET streaming_parallelism_strategy_for_materialized_view = 'RATIO(0.5)';
```

## System-Level Configuration

The system-level adaptive parallelism strategy can be set via:

```sql
-- Requires superuser privileges
ALTER SYSTEM SET adaptive_parallelism_strategy = 'AUTO';
```

This affects the default behavior when `streaming_parallelism_strategy = 'default'`.

## Max Parallelism Limit

The maximum parallelism for any streaming job is controlled by:

```sql
SET streaming_max_parallelism = 256;
```

**Important**: This setting controls the virtual node count and cannot be changed after materialized views are created. It affects:
- Maximum future parallelism after scaling
- Range scan performance (higher values may degrade performance)

**Recommendation**: Use the default (256) unless you have specific requirements.

## Checking Current Configuration

```sql
-- View all parallelism settings
SHOW streaming_parallelism;
SHOW streaming_parallelism_strategy;
SHOW streaming_parallelism_for_source;
SHOW streaming_parallelism_strategy_for_source;
-- ... etc for other types

-- View system catalog
SELECT * FROM rw_streaming_parallelism;
```

## Best Practices

1. **Start with defaults**: Sources and tables already default to `BOUNDED(4)`
   ```sql
   SET streaming_parallelism = 'adaptive';
   -- No need to explicitly set source and table strategies
   ```

2. **Override if needed**: Increase limits for specific use cases
   ```sql
   -- Increase source parallelism if needed
   SET streaming_parallelism_strategy_for_source = 'BOUNDED(8)';
   ```

3. **Use RATIO for flexibility**: Balance resources across multiple MVs
   ```sql
   SET streaming_parallelism_strategy_for_materialized_view = 'RATIO(0.5)';
   ```

4. **Fixed parallelism for predictability**: Use when you need consistent resource usage
   ```sql
   SET streaming_parallelism_for_table = 4;
   ```

5. **Monitor and adjust**: Check actual parallelism and adjust based on performance
   ```sql
   SELECT * FROM rw_streaming_parallelism;
   ```

## Migration Guide

### From Fixed to Adaptive

**Before**:
```sql
SET streaming_parallelism = 8;
```

**After**:
```sql
SET streaming_parallelism = 'adaptive';
SET streaming_parallelism_strategy = 'BOUNDED(8)';
```

This allows the system to use fewer resources when the cluster is smaller, while capping at 8 when the cluster grows.

### Adding Type-Specific Overrides

**Before**:
```sql
SET streaming_parallelism = 8;
```

**After**:
```sql
SET streaming_parallelism = 'adaptive';
SET streaming_parallelism_strategy_for_source = 'BOUNDED(4)';
SET streaming_parallelism_strategy_for_table = 'BOUNDED(4)';
SET streaming_parallelism_strategy_for_materialized_view = 'RATIO(0.5)';
```

This provides fine-grained control while maintaining adaptive behavior.

## See Also

- [Streaming Engine Overview](./streaming-overview.md)
- [Checkpoint](./checkpoint.md)
- [Backfill](./backfill.md)
