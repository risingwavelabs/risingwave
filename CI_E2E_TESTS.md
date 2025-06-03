# CI E2E Test Configuration Changes

## Overview

This document describes the changes made to optimize CI resource usage by removing the duplication of e2e tests running both in serial and parallel modes.

## Issue

Previously, RisingWave CI was running e2e tests with significant overlap between serial and parallel execution:
- **Streaming tests**: Both serial and parallel scripts ran `./e2e_test/streaming/**/*.slt`
- **Basic batch tests**: Both ran DDL and visibility_mode tests
- This duplication wasted CI resources and time

Reference: [GitHub Issue #18992](https://github.com/risingwavelabs/risingwave/issues/18992)

## Analysis of Test Coverage

### Serial e2e tests (`e2e-test-serial.sh`):
- **Comprehensive coverage** for standalone, single-node, and cluster modes
- **Unique tests**: misc, python_client, subscription, superset, error_ui, extended_mode
- **Mode-specific tests**: standalone persistence and cluster options tests
- **Additional tests**: ttl, dml, background_ddl, backfill tests

### Parallel e2e tests (`e2e-test-parallel.sh`):
- **Fast execution** for cluster mode only using parallel execution (-j 16)
- **Focused coverage**: streaming, basic batch, udf, generated tests
- **Optimized for speed** with multiple frontend ports

### Overlapping Tests (Duplication):
- Streaming tests: `./e2e_test/streaming/**/*.slt`
- Basic batch tests: DDL and visibility_mode tests

## Solution Implemented

### Modified `ci/scripts/e2e-test-serial.sh`

**Strategy**: Remove overlapping tests from serial script when running in cluster mode, while preserving all unique test coverage.

**Changes**:
1. **For standalone/single-node modes**: Run full test suite including streaming and batch tests (no duplication since parallel tests don't support these modes)
2. **For cluster mode**: Skip streaming and basic batch tests (covered by parallel), but run all unique tests:
   - backfill tests not covered by parallel
   - misc, python_client, subscription, superset tests
   - error_ui and extended_mode tests
   - ttl, dml, background_ddl tests

**Before**:
```bash
# Always ran streaming and batch tests regardless of mode
echo "--- e2e, $mode, streaming"
risedev slt -p 4566 -d dev './e2e_test/streaming/**/*.slt'

echo "--- e2e, $mode, batch"  
risedev slt -p 4566 -d dev './e2e_test/ddl/**/*.slt'
risedev slt -p 4566 -d dev './e2e_test/visibility_mode/*.slt'
```

**After**:
```bash
# Skip streaming and basic batch tests for cluster mode since parallel tests cover these
if [[ "$mode" == "standalone" || "$mode" == "single-node" ]]; then
  # Full test suite for non-cluster modes
  echo "--- e2e, $mode, streaming"
  risedev slt -p 4566 -d dev './e2e_test/streaming/**/*.slt'
  
  echo "--- e2e, $mode, batch"
  risedev slt -p 4566 -d dev './e2e_test/ddl/**/*.slt'
  risedev slt -p 4566 -d dev './e2e_test/visibility_mode/*.slt'
else
  # For cluster mode, only run tests not covered by parallel
  echo "--- e2e, $mode, additional batch tests"
  risedev slt -p 4566 -d dev './e2e_test/background_ddl/basic.slt'
  risedev slt -p 4566 -d dev './e2e_test/ttl/ttl.slt'
  risedev slt -p 4566 -d dev './e2e_test/dml/*.slt'
fi

# Always run unique tests not covered by parallel
echo "--- e2e, $mode, misc"
echo "--- e2e, $mode, python_client"  
echo "--- e2e, $mode, subscription"
echo "--- e2e, $mode, Apache Superset"
```

## Impact

### Performance Improvements:
- **Eliminated Duplication**: Streaming and basic batch tests no longer run twice in cluster mode
- **Preserved Coverage**: All unique test scenarios are still executed
- **Faster CI**: Reduced overall CI execution time by eliminating redundant test runs
- **Resource Savings**: Lower compute resource consumption per CI run

### Test Coverage Matrix:

| Test Category | Standalone | Single-Node | Cluster (Serial) | Cluster (Parallel) |
|---------------|------------|-------------|------------------|-------------------|
| Streaming | ✅ Serial | ✅ Serial | ❌ (covered by parallel) | ✅ Parallel |
| DDL/Visibility | ✅ Serial | ✅ Serial | ❌ (covered by parallel) | ✅ Parallel |
| UDF | ❌ | ❌ | ❌ | ✅ Parallel |
| Generated | ❌ | ❌ | ❌ | ✅ Parallel |
| Misc | ✅ Serial | ✅ Serial | ✅ Serial | ❌ |
| Python Client | ✅ Serial | ✅ Serial | ✅ Serial | ❌ |
| Subscription | ✅ Serial | ✅ Serial | ✅ Serial | ❌ |
| Superset | ✅ Serial | ✅ Serial | ✅ Serial | ❌ |
| Error UI | ❌ | ❌ | ✅ Serial | ❌ |
| Extended Mode | ✅ Serial | ✅ Serial | ✅ Serial | ❌ |
| TTL/DML | ✅ Serial | ✅ Serial | ✅ Serial | ❌ |
| Backfill | ✅ Serial | ✅ Serial | ✅ Serial | ❌ |

### Migration Impact:

#### For Regular Development:
- ✅ **No action required** - all tests continue to run with same coverage
- ✅ **Faster feedback** - reduced CI execution time  
- ✅ **Same triggers** - existing `ci/run-e2e-tests` label continues to work

#### For CI Infrastructure:
- ✅ **Resource optimization** - eliminated duplicate test execution
- ✅ **Maintained reliability** - no reduction in test coverage
- ✅ **Improved efficiency** - better resource utilization

## Technical Details

The solution ensures that:
1. **No test coverage is lost** - every test still runs, just without duplication
2. **Mode-specific optimizations** - standalone and single-node get full coverage, cluster mode avoids duplication
3. **Parallel optimization** - cluster mode leverages fast parallel execution for core tests
4. **Unique coverage preserved** - serial-only tests (misc, python_client, etc.) continue to run

## Verification

To verify the changes work correctly:

1. **Standalone mode**: Should run full test suite including streaming and batch
2. **Single-node mode**: Should run full test suite including streaming and batch  
3. **Cluster mode**: Should skip streaming/basic batch tests but run all unique tests
4. **Parallel tests**: Should continue running streaming, batch, udf, and generated tests

## Next Steps

1. Monitor CI performance improvements after deployment
2. Consider migrating more serial-only tests to parallel execution for further optimization
3. Evaluate opportunities to parallelize standalone and single-node test execution