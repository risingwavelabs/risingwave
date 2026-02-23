# AGENTS.md - PostgreSQL Regression Test Data Files

## 1. Scope

Policies for `src/tests/regress/data/data` - raw data files for PostgreSQL regression testing, primarily for COPY command and bulk loading tests.

## 2. Purpose

This directory contains data files used by regression tests:

- **COPY command data**: Raw data files for COPY FROM testing
- **Bulk loading**: Large datasets for performance and correctness testing
- **Format testing**: Various data formats (CSV, TSV, binary)
- **Sample datasets**: Employee/department data for relational tests
- **Arrays and JSON**: Data files with complex types

Data files provide realistic test content for COPY and bulk operations.

## 3. Structure

```
src/tests/regress/data/data/
├── agg.data                  # Aggregation test data
├── array.data                # Array test data
├── constrf.data              # Foreign key constraint data
├── constro.data              # Constraint operations data
├── dept.data                 # Department table data
├── emp.data                  # Employee table data
├── hash.data                 # Hash join test data
├── jsonb.data                # JSONB test data
├── onek.data                 # 1000-row test dataset
├── person.data               # Person table data
├── real_city.data            # Geographic data
├── streets.data              # Street name data
├── student.data              # Student table data
├── tenk.data                 # 10000-row test dataset
└── *.data                    # Additional data files
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `emp.data` | Employee records for join/aggregate tests |
| `dept.data` | Department records for foreign key tests |
| `tenk.data` | 10,000 row dataset for scale testing |
| `onek.data` | 1,000 row dataset for medium tests |
| `array.data` | Array type test data |
| `jsonb.data` | JSONB document test data |
| `agg.data` | Aggregation test dataset |
| `hash.data` | Hash function test data |

## 5. Edit Rules (Must)

- Maintain tab-separated values (TSV) format for most files
- Preserve row counts for consistent test results
- Update corresponding expected outputs when data changes
- Document data schema in comments
- Follow PostgreSQL data file conventions

## 6. Forbidden Changes (Must Not)

- Change data format without updating COPY commands
- Remove data files used by active tests
- Corrupt data integrity (check for proper escaping)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| COPY tests | Via regress test runner |
| Data loading | Tests using `COPY ... FROM` with these files |

## 8. Dependencies & Contracts

- Format: Primarily tab-separated values
- Encoding: UTF-8 text
- Usage: Referenced from SQL files via COPY command
- Correspondence: Linked to specific SQL test files

## 9. Overrides

None. Inherits all rules from parent `./src/tests/regress/data/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New data formats added
- Data file organization changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/tests/regress/data/AGENTS.md
