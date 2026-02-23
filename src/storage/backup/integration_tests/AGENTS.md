# AGENTS.md - Backup Integration Tests

## 1. Scope

Policies for `src/storage/backup/integration_tests` - integration test suite for RisingWave's backup and restore functionality, validating end-to-end backup operations with real clusters.

## 2. Purpose

This directory contains shell-based integration tests for backup/restore:

- **End-to-end validation**: Tests backup/restore with actual RisingWave clusters
- **Object store integration**: Validates S3-compatible storage backends (MinIO)
- **Meta snapshot testing**: Tests metadata backup and restore operations
- **Hummock integration**: Validates SSTable lifecycle during backup
- **Recovery scenarios**: Tests disaster recovery and point-in-time restore
- **Configuration testing**: Validates backup configuration options

Integration tests ensure backup/restore works correctly in realistic deployment scenarios.

## 3. Structure

```
src/storage/backup/integration_tests/
├── run_all.sh                    # Entry point to run all integration tests
├── common.sh                     # Shared utilities and functions
├── test_basic.sh                 # Basic backup and restore test
├── test_pin_sst.sh               # SSTable pinning during backup test
├── test_query_backup.sh          # Query backup data test
├── test_set_config.sh            # Backup configuration test
├── test_overwrite_endpoint.sh    # Object store endpoint override test
├── Makefile.toml                 # RiseDev task definitions
└── .gitignore                    # Excludes test artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `run_all.sh` | Main entry point, runs all integration tests sequentially |
| `common.sh` | Shared functions: `start_cluster`, `stop_cluster`, `create_mvs`, `query_mvs`, `full_gc_sst` |
| `test_basic.sh` | Basic backup creation, restore, and data verification |
| `test_pin_sst.sh` | Tests SSTable pinning prevents garbage collection during backup |
| `test_query_backup.sh` | Tests querying backup metadata and contents |
| `test_set_config.sh` | Tests backup configuration setting and retrieval |
| `test_overwrite_endpoint.sh` | Tests object store endpoint override functionality |
| `Makefile.toml` | RiseDev integration for CI pipeline |

## 5. Edit Rules (Must)

- Source `common.sh` for shared utilities in all test scripts
- Use `set -eo pipefail` for strict error handling
- Clean up clusters with `stop_cluster` and `clean_all_data` after tests
- Use `start_cluster` for consistent test environment setup
- Create materialized views with `create_mvs` for test data
- Query data with `query_mvs` for verification
- Use `full_gc_sst` to trigger SSTable garbage collection
- Document test purpose and steps in comments
- Return non-zero exit code on failure
- Run `./run_all.sh` before submitting changes

## 6. Forbidden Changes (Must Not)

- Skip cluster cleanup after tests
- Hardcode connection parameters (use environment variables)
- Modify common.sh functions without updating dependent tests
- Remove test coverage for existing backup features
- Use production object stores for testing
- Leave test data in shared environments
- Ignore test failures without investigation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All integration tests | `bash integration_tests/run_all.sh` |
| Single test | `bash integration_tests/test_basic.sh` |
| With RiseDev | `cargo make run-backup-integration-tests` |
| CI pipeline | Runs automatically in backup-related CI jobs |

## 8. Dependencies & Contracts

- Environment: `BACKUP_TEST_MCLI`, `BACKUP_TEST_MCLI_CONFIG`, `BACKUP_TEST_RW_ALL_IN_ONE`
- Object store: MinIO for S3-compatible testing
- Database: SQLite for meta store testing
- Tools: `mc` (MinIO client), `sqlite3`, `psql`, `risectl`
- Cluster: RiseDev `ci-meta-backup-test-sql` profile
- Common functions: Defined in `common.sh`

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/storage/backup/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New integration test added
- Common.sh utilities modified
- Test environment setup changed
- Backup/restore CLI interface updated
- New test scenarios identified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/storage/backup/AGENTS.md
