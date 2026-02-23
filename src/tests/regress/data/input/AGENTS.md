# AGENTS.md - PostgreSQL Regression Alternative Input Files

## 1. Scope

Policies for `src/tests/regress/data/input` - alternative input files for PostgreSQL regression tests requiring different setup or preconditions.

## 2. Purpose

This directory contains alternative input sources for specific test scenarios:

- **Alternative inputs**: Different input files for complex test setups
- **Large object tests**: Files for testing BLOB/CLOB operations
- **Constraint tests**: Files for referential integrity testing
- **Copy tests**: Data files for COPY command testing
- **Tablespace tests**: Files for tablespace-related tests
- **Supplementary data**: Additional inputs not in standard SQL format

Input files provide data or alternative SQL for specialized test cases.

## 3. Structure

```
src/tests/regress/data/input/
├── constraints.source        # Constraint test setup
├── copy.source               # COPY command test data
├── create_function_0.source  # Function creation tests (part 1)
├── create_function_1.source  # Function creation tests (part 2)
├── create_function_2.source  # Function creation tests (part 3)
├── largeobject.source        # Large object (BLOB) tests
├── misc.source               # Miscellaneous test data
└── tablespace.source         # Tablespace test setup
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `constraints.source` | Foreign key and constraint test setup |
| `copy.source` | Data for COPY FROM/TO testing |
| `create_function_*.source` | PL/pgSQL function definitions for testing |
| `largeobject.source` | Large object manipulation tests |
| `misc.source` | Additional test data |
| `tablespace.source` | Tablespace creation and usage |

## 5. Edit Rules (Must)

- Follow PostgreSQL `.source` file format conventions
- Keep files synchronized with SQL test expectations
- Document RisingWave-specific adaptations
- Run tests after modifications

## 6. Forbidden Changes (Must Not)

- Remove files without updating test schedule
- Break PostgreSQL source file format
- Skip test validation for modified files

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run tests | Via regress test runner with schedule file |

## 8. Dependencies & Contracts

- Format: PostgreSQL source file format
- Schedule: Referenced from `../schedule`
- Runner: `risingwave_regress_test`

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/tests/regress/data/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New input file types added
- Format conventions changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/tests/regress/data/AGENTS.md
