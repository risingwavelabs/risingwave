# AGENTS.md - SQL Parser Examples

## 1. Scope

Directory: `src/sqlparser/examples`

Example programs demonstrating SQL parser usage for RisingWave.

## 2. Purpose

Provides practical CLI examples for developers learning to use the RisingWave SQL parser:
- Parse SQL files and display AST representation
- Demonstrate tokenizer and parser API usage
- Serve as integration examples for external tools

## 3. Structure

```
src/sqlparser/examples/
├── parse.rs              # CLI example: parse SQL from stdin or file
└── AGENTS.md             # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `parse.rs` | Example CLI binary for parsing SQL statements |

## 5. Edit Rules (Must)

- Keep examples simple and well-documented for learning purposes
- Demonstrate core parser APIs: `Parser::parse_sql()`, tokenizer usage
- Handle errors gracefully with helpful error messages
- Use `std::io` for stdin/file reading to be universally applicable
- Include comments explaining key API calls
- Run `cargo fmt` before committing
- Test examples compile: `cargo build --example parse`

## 6. Forbidden Changes (Must Not)

- Add complex dependencies beyond parser crate
- Include production logic or business rules
- Use unsafe code blocks
- Hardcode file paths or credentials
- Remove stdin support (keep examples flexible)
- Break compatibility with standard cargo example conventions

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build example | `cargo build --example parse -p risingwave_sqlparser` |
| Run example | `cargo run --example parse -p risingwave_sqlparser -- "SELECT 1"` |
| Check examples | `cargo check --examples -p risingwave_sqlparser` |

## 8. Dependencies & Contracts

- Inherits dependencies from parent `risingwave_sqlparser` crate
- Uses standard library for I/O operations
- Demonstrates public API only (no internal modules)
- Examples are compiled during `cargo build --examples`

## 9. Overrides

None. Inherits from parent `src/sqlparser/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New example binaries added
- Parser public API changes affecting examples
- New SQL parsing features demonstrated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/src/sqlparser/AGENTS.md`
