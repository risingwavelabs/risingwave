# AGENTS.md - SQLSmith Test Data

## 1. Scope

Policies for `src/tests/sqlsmith/tests/testdata` - schema definitions and seed data for the SQLSmith SQL fuzzing framework.

## 2. Purpose

This directory provides test schemas for SQLSmith's query generation:

- **Schema definitions**: CREATE TABLE statements defining test database structure
- **Type coverage**: Tables with all RisingWave data types
- **Relationship modeling**: Tables with foreign key relationships for join testing
- **Complex types**: Arrays, structs, and composite types
- **Benchmark schemas**: TPC-H and Nexmark table definitions
- **Generation context**: Schemas guide SQLSmith's random query generation

Test data provides the foundation for generating semantically valid random SQL.

## 3. Structure

```
src/tests/sqlsmith/tests/testdata/
├── alltypes.sql              # All data types for comprehensive testing
├── tpch.sql                  # TPC-H benchmark schema
├── nexmark.sql               # Nexmark streaming benchmark schema
├── drop_alltypes.sql         # Cleanup for alltypes tables
├── drop_nexmark.sql          # Cleanup for Nexmark tables
└── drop_tpch.sql             # Cleanup for TPC-H tables
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `alltypes.sql` | Comprehensive type coverage (boolean, int, float, decimal, date, time, timestamp, interval, struct, array, varchar) |
| `tpch.sql` | TPC-H benchmark tables (customer, orders, lineitem, part, supplier, etc.) |
| `nexmark.sql` | Nexmark streaming tables (person, auction, bid) with watermarks |
| `drop_*.sql` | Cleanup scripts for test teardown |

## 5. Edit Rules (Must)

- Use valid RisingWave SQL CREATE TABLE syntax
- Include all data types supported by RisingWave
- Define primary keys where appropriate
- Add watermarks for streaming test tables
- Use APPEND ONLY for high-throughput tables
- Keep schemas synchronized with SQL generator capabilities
- Update drop scripts when adding new tables
- Test schemas with `./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata`
- Document unsupported types in comments

## 6. Forbidden Changes (Must Not)

- Remove type coverage without justification
- Add types not supported by RisingWave
- Break referential integrity in benchmark schemas
- Remove tables without updating SQL generator
- Skip drop script updates

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| SQLSmith run | `./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata` |
| Differential testing | `./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata --differential-testing` |
| Frontend tests | `cargo test -p risingwave_sqlsmith --features enable_sqlsmith_unit_test` |

## 8. Dependencies & Contracts

- SQLSmith: Reads schema to generate context-aware queries
- RisingWave: Schemas must be valid for target database
- Test runner: Executes SQL against running RisingWave instance
- Format: Standard SQL DDL statements

## 9. Overrides

None. Follows parent AGENTS.md at `./src/tests/sqlsmith/tests/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New data types added to RisingWave
- Schema format changes
- New benchmark schemas added
- SQLSmith generation capabilities expanded

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/tests/sqlsmith/tests/AGENTS.md
