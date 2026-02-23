# AGENTS.md - SQLSmith Fuzzing Tests

## 1. Scope

Directory: `src/tests/sqlsmith`
Crate: `risingwave_sqlsmith`

SQL fuzzing test framework for discovering bugs through randomized query generation.

## 2. Purpose

Automated testing tool for finding edge cases and bugs:
- Generate valid SQL queries based on RisingWave's supported feature set
- Discover unexpected panics and crashes in query processing
- Test frontend components: parser, binder, planner, optimizer
- Perform differential testing between batch and streaming queries
- Support deterministic fuzzing with seed-based generation
- Reduce failing queries to minimal reproducible cases

## 3. Structure

```
src/tests/sqlsmith/
├── src/
│   ├── lib.rs              # Core SQL generation and test runner
│   ├── bin/
│   │   └── sqlsmith.rs     # CLI entry point
│   ├── config.rs           # Test configuration and feature flags
│   ├── reducer.rs          # Query minimization for bug reproduction
│   ├── utils.rs            # Utility functions
│   ├── validation.rs       # Query validation and error classification
│   ├── sql_gen/            # SQL generation modules
│   │   ├── *.rs            # Expression, query, DDL generators
│   └── test_runners/       # Test execution implementations
│       └── *.rs            # Frontend, e2e, differential test runners
├── tests/
│   ├── testdata/           # Test schemas and seed data
│   └── test_runner.rs      # Unit test runner
├── scripts/                # Supporting scripts
├── develop.md              # Developer guide for adding features
├── README.md               # User documentation
└── AGENTS.md               # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | SQL generator core and fuzzing orchestration |
| `src/reducer.rs` | Automatic query minimization for bug reports |
| `src/validation.rs` | Error classification and validation logic |
| `src/sql_gen/*.rs` | Modular SQL statement generators |
| `src/test_runners/*.rs` | Frontend, E2E, and differential test runners |
| `tests/testdata/` | Schema definitions and test data |
| `develop.md` | Guide for extending SQLSmith with new features |

## 5. Edit Rules (Must)

- Add new SQL features to generators incrementally
- Ensure generated SQL is syntactically valid
- Use feature flags in `config.rs` to control generation
- Support both batch and streaming query generation
- Implement proper error classification in `validation.rs`
- Add query reduction support for new statement types
- Document new features in `develop.md`
- Run `cargo fmt` before committing
- Test with madsim simulation for deterministic results
- Update function signatures when adding new SQL functions

## 6. Forbidden Changes (Must Not)

- Remove error classification without replacement
- Break deterministic fuzzing (must support seed-based generation)
- Skip query reduction for new statement types
- Generate invalid SQL syntax intentionally
- Remove differential testing between batch/streaming
- Modify without testing with madsim simulation
- Break backward compatibility with existing test schemas

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Frontend unit tests | `./risedev test -E "package(risingwave_sqlsmith)" --features enable_sqlsmith_unit_test` |
| Madsim simulation | `cargo make sslt-build-all --profile ci-sim && MADSIM_TEST_SEED=1 ./target/sim/ci-sim/risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata` |
| E2E fuzzing | `./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata` |
| Differential testing | `./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata --differential-testing` |
| Generate snapshots | Automated in CI, see README.md for manual reproduction |

## 8. Dependencies & Contracts

- `risingwave_frontend`: Frontend testing (binder, planner)
- `risingwave_sqlparser`: SQL parsing validation
- `madsim`: Deterministic simulation for reproducible tests
- `tokio-postgres`: E2E testing with actual queries
- `libfuzzer-sys`: For coverage-guided fuzzing integration
- Uses YAML config for feature toggles
- Query generation must respect RisingWave feature support matrix

## 9. Overrides

None. Follows parent test policies from `src/tests/` hierarchy.

## 10. Update Triggers

Regenerate this file when:
- New SQL features added to generators
- Query reduction logic enhanced
- Test runner modes modified (frontend/e2e/differential)
- Madsim integration changes
- New bug discovery patterns introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./AGENTS.md`
