# AGENTS.md - SQL Parser Fuzz Tests

## 1. Scope

Policies for the SQL parser fuzz testing crate (`src/sqlparser/fuzz`), providing fuzzing infrastructure for the RisingWave SQL parser.

## 2. Purpose

This crate provides:
- Fuzzing targets for SQL parsing using honggfuzz
- Automated discovery of parser bugs and edge cases
- Crash reproduction and regression testing
- Coverage-guided fuzzing for SQL syntax

## 3. Structure

```
fuzz/
├── Cargo.toml              # Fuzz crate manifest (standalone workspace)
└── fuzz_targets/
    └── fuzz_parse_sql.rs   # SQL parser fuzzing target
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Fuzz crate configuration with honggfuzz dependency |
| `fuzz_targets/fuzz_parse_sql.rs` | Main fuzzing target for SQL parsing |

## 5. Edit Rules (Must)

- Use `honggfuzz` crate for fuzzing infrastructure
- Handle all parser errors gracefully (no panics)
- Add corpus seeds for new SQL features
- Run fuzz tests after parser changes

## 6. Forbidden Changes (Must Not)

- Remove fuzz targets without justification
- Disable fuzzing in CI without approval
- Introduce non-determinism in fuzz targets
- Panic in fuzz targets (use proper error handling)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run fuzzer | `cargo hfuzz run fuzz_parse_sql` |
| Fuzz tests | `cargo fuzz run fuzz_parse_sql` |

## 8. Dependencies & Contracts

- `honggfuzz`: Fuzzing framework
- `risingwave_sqlparser`: Parser under test
- Standalone workspace (not part of main workspace)

## 9. Overrides

None. Follows parent AGENTS.md policies.

## 10. Update Triggers

Regenerate this file when:
- New fuzz targets added
- Fuzzing framework changes
- Parser API changes affecting fuzz targets

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/sqlparser/AGENTS.md
