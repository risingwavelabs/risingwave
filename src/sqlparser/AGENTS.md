# AGENTS.md - SQL Parser Crate

## 1. Scope

Directory: `/home/k11/risingwave/src/sqlparser`
Crate: `risingwave_sqlparser`

SQL parser for RisingWave - converts SQL strings into Abstract Syntax Tree (AST).

**Policy Inheritance:**
This file inherits rules from `/home/k11/risingwave/AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

ANSI SQL:2011 compatible SQL lexer and parser. Forked from sqlparser-rs.
Provides:
- Tokenizer (lexer) for SQL lexical analysis
- Parser for SQL to AST conversion
- AST types for all SQL constructs
- Pretty-printing (unparsing) from AST back to SQL

## 3. Structure

```
src/
├── lib.rs              # Crate entry point
├── tokenizer.rs        # SQL lexer - converts SQL to tokens
├── parser.rs           # Main SQL parser - tokens to AST
├── parser_v2/          # New parser implementation (winnow-based)
│   ├── mod.rs          # TokenStream traits and combinators
│   ├── expr.rs         # Expression parsing
│   ├── data_type.rs    # Data type parsing
│   └── impl_.rs        # Implementation details
├── keywords.rs         # SQL keyword definitions
├── quote_ident.rs      # Identifier quoting utilities
├── ast/                # Abstract Syntax Tree types
│   ├── mod.rs          # Core AST types and Display impls
│   ├── statement.rs    # SQL statement AST
│   ├── query.rs        # Query AST (SELECT, CTEs, JOINs)
│   ├── ddl.rs          # DDL AST (CREATE, ALTER, DROP)
│   ├── data_type.rs    # Data type definitions
│   ├── value.rs        # Literal values
│   ├── operator.rs     # SQL operators
│   ├── analyze.rs      # ANALYZE statement
│   └── legacy_source.rs# Legacy source format handling
└── bin/sqlparser.rs    # CLI binary

tests/
├── parser_test.rs      # YAML-based test runner
├── sqlparser_common.rs # Common parser tests
├── sqlparser_postgres.rs # Postgres-specific tests
└── testdata/           # YAML test cases

examples/parse.rs       # CLI example
fuzz/                   # Fuzzing targets
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Public API exports |
| `src/tokenizer.rs` | SQL lexer (Token, Tokenizer) |
| `src/parser.rs` | Main Parser type and parsing logic |
| `src/parser_v2/mod.rs` | Winnow-based parser combinators |
| `src/keywords.rs` | Keyword definitions and reserved words |
| `src/ast/mod.rs` | Core AST types (Statement, Query, Expr) |
| `src/ast/statement.rs` | All SQL statement variants |
| `tests/testdata/*.yaml` | Parser test cases |

## 5. Edit Rules (Must)

- Add new SQL syntax in `parser.rs` or `parser_v2/` modules
- Add new AST types in appropriate `ast/*.rs` files
- Add new keywords to `keywords.rs` (keep sorted for binary search)
- Run `./risedev update-parser-test` after modifying parser behavior
- Write all code comments in English
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Break backward compatibility without explicit migration path
- Modify parser without updating YAML test expectations
- Delete or rename existing AST fields without deprecation
- Bypass safety checks with `unsafe` blocks
- Add keywords without checking reserved word lists

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Parser unit tests | `cargo test -p risingwave_sqlparser` |
| Update test expectations | `./risedev update-parser-test` |
| YAML parser tests | `cargo test --test parser_test` |
| Fuzz tests | `cargo fuzz run fuzz_parse_sql` |
| Bench | `cargo bench -p risingwave_sqlparser` |

## 8. Dependencies & Contracts

- Primary: `winnow` (parser combinators)
- AST uses `itertools`, `thiserror`, `tracing`
- Tokenizer produces `Vec<TokenWithLocation>`
- Parser produces `Vec<Statement>` from SQL string
- All AST types implement `Display` for unparsing

## 9. Overrides

None. Inherits from parent AGENTS.md.

## 10. Update Triggers

Regenerate this file when:
- New AST types added
- Parser architecture changes
- Keyword handling modified
- New test framework introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md
