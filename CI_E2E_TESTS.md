# CI E2E Test Configuration Changes

## Overview

This document describes the changes made to optimize CI resource usage by removing the duplication of e2e tests running both in serial and parallel modes.

## Issue

Previously, RisingWave CI was running e2e tests in both serial and parallel modes by default, which was:
- Wasting CI resources and time
- Running redundant tests
- Making CI slower than necessary

Reference: [GitHub Issue #18992](https://github.com/risingwavelabs/risingwave/issues/18992)

## Changes Made

### 1. Modified `ci/workflows/pull-request.yml`

**Before:**
- Serial e2e test ran by default for all PRs (unless `ci/pr/run-selected` label was present)
- Parallel e2e test also ran by default
- Both tests were triggered by the same `ci/run-e2e-tests` label

**After:**
- Serial e2e test now only runs when explicitly requested via `ci/run-e2e-tests-serial` label
- Parallel e2e test continues to run by default and is triggered by `ci/run-e2e-tests` label
- This eliminates the duplication while preserving the ability to run serial tests when needed for debugging

### 2. Modified `ci/workflows/main-cron.yml`

**Before:**
- Serial e2e test ran by default in the main cron workflow
- Parallel e2e test also ran by default

**After:**
- Serial e2e test now only runs when explicitly requested via `ci/run-e2e-tests-serial` label
- Parallel e2e test continues to run by default
- Maintained the slow e2e test configuration unchanged

## Impact

### Positive Impact:
- **Faster CI**: Eliminates redundant test execution, reducing overall CI time
- **Resource Savings**: Reduces compute resources used by CI infrastructure
- **Maintained Coverage**: Parallel tests provide the same test coverage as serial tests
- **Debugging Capability**: Serial tests can still be run when needed for debugging specific issues

### Migration Guide:

#### For Regular Development:
- **No action needed** - parallel e2e tests will continue to run by default
- Use the existing `ci/run-e2e-tests` label to trigger e2e tests

#### For Debugging:
- Use the new `ci/run-e2e-tests-serial` label to run serial e2e tests when needed
- This is useful for debugging race conditions or issues that might be masked in parallel execution

## Labels Reference

| Label | Purpose | When to Use |
|-------|---------|-------------|
| `ci/run-e2e-tests` | Run parallel e2e tests | Default for most cases |
| `ci/run-e2e-tests-serial` | Run serial e2e tests | Debugging specific issues |
| `ci/run-e2e-parallel-tests` | Alternative label for parallel tests | Legacy compatibility |

## Technical Details

The changes ensure that:
1. **Default behavior** runs only parallel e2e tests, which are faster and more efficient
2. **Serial tests are available** when explicitly requested for debugging
3. **No test coverage is lost** - parallel tests execute the same test suites
4. **Standalone and single-node tests** remain unchanged as they serve specific purposes

## Future Considerations

- Monitor CI performance improvements after these changes
- Consider further optimizations based on usage patterns
- Evaluate if serial tests are still needed long-term based on debugging usage