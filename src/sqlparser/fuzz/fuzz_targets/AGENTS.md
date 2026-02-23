# AGENTS.md - SQL Parser Fuzz Targets

## 1. Scope

Policies for `/home/k11/risingwave/src/sqlparser/fuzz/fuzz_targets` - fuzzing targets for automated SQL parser testing.

## 2. Purpose

The fuzz targets provide:
- Coverage-guided fuzzing for SQL parsing using honggfuzz
- Automated discovery of parser panics and crashes
- Regression testing for previously found bugs
- Continuous fuzzing integration for security and stability

Fuzzing complements unit tests by exploring edge cases in the input space that manual testing might miss.

## 3. Structure

```
src/sqlparser/fuzz/fuzz_targets/
├── fuzz_parse_sql.rs         # SQL parser fuzzing target
└── AGENTS.md                 # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `fuzz_parse_sql.rs` | Fuzzing target for SQL parsing (honggfuzz) |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Use `honggfuzz::fuzz!` macro for fuzzing harness
- Handle all parser errors gracefully (no panics in target)
- Use `GenericDialect` for dialect-agnostic fuzzing
- Keep fuzz targets simple and focused
- Document fuzzing target purpose in code comments
- Ensure fuzz targets compile: `cargo check -p risingwave_sqlparser_fuzz`
- Add corpus seeds for new SQL features to improve coverage

## 6. Forbidden Changes (Must Not)

- Remove fuzz targets without justification
- Panic in fuzz targets (use proper error handling)
- Introduce non-determinism in fuzz targets
- Disable fuzzing in CI without approval
- Add complex setup that slows fuzzing iteration
- Use blocking I/O in fuzz targets
- Ignore parser errors without attempting parse

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run fuzzer | `cargo hfuzz run fuzz_parse_sql` |
| Check fuzz target | `cargo check -p risingwave_sqlparser_fuzz` |
| Fuzz with libfuzzer | `cargo fuzz run fuzz_parse_sql` |

## 8. Dependencies & Contracts

- `honggfuzz`: Fuzzing framework (primary)
- `libfuzzer`: Alternative fuzzing framework (via `cargo-fuzz`)
- `risingwave_sqlparser`: Parser under test
- `GenericDialect`: Dialect for dialect-agnostic fuzzing
- Fuzz target format: `fn main()` with `fuzz!(|data: String| { ... })`
- Fuzzing loop: Infinite loop with corpus mutation

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/sqlparser/fuzz/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New fuzz targets added
- Fuzzing framework changes (honggfuzz/libfuzzer)
- Parser API changes affecting fuzz targets
- New fuzzing strategies introduced
- Corpus format changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/sqlparser/fuzz/AGENTS.md
