# AGENTS.md - SQL Parser Benchmarks

## 1. Scope

Policies for `/home/k11/risingwave/src/sqlparser/sqlparser_bench/benches` - Criterion-based performance benchmarks for the RisingWave SQL parser.

## 2. Purpose

The SQL parser benchmark suite measures:
- SQL parsing throughput for various query types
- Tokenizer (lexer) performance
- Parser performance for complex queries (WITH clauses, subqueries)
- Performance regression detection
- Data for parser optimization decisions

Benchmarks help ensure parser performance remains acceptable as SQL syntax support expands.

## 3. Structure

```
src/sqlparser/sqlparser_bench/benches/
├── sqlparser_bench.rs        # Criterion benchmark definitions
└── AGENTS.md                 # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `sqlparser_bench.rs` | Criterion benchmark cases for SQL parsing |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Use `criterion` for statistical benchmarking
- Include diverse SQL workloads: simple queries, complex joins, CTEs
- Use static SQL strings for reproducible results
- Use `black_box` to prevent compiler optimizations skewing results
- Document benchmark SQL complexity and purpose in comments
- Benchmark both tokenizer and full parser pipeline where applicable
- Run `cargo check --benches -p risingwave_sqlparser` before submitting
- Ensure benchmarks compile in CI

## 6. Forbidden Changes (Must Not)

- Remove statistical rigor (sample sizes, warm-up iterations)
- Benchmark only trivial SQL (must represent real query workloads)
- Hardcode file paths for test SQL
- Skip CI benchmark compilation checks
- Use `#[bench]` (deprecated) instead of criterion
- Add I/O operations inside benchmark loops
- Use non-deterministic data generation
- Remove existing benchmark coverage

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run benchmarks | `cargo bench -p risingwave_sqlparser` |
| Check benches | `cargo check --benches -p risingwave_sqlparser` |
| Specific benchmark | `cargo bench -p risingwave_sqlparser -- <name>` |

## 8. Dependencies & Contracts

- `criterion`: Statistical benchmarking framework
- `risingwave_sqlparser`: Parser under test
- Benchmark groups organized by query complexity
- Static SQL strings for reproducible benchmarks
- Output saved to `target/criterion/` with HTML reports

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/sqlparser/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New benchmark cases added
- Benchmark framework changes (criterion version)
- Parser API changes affecting benchmark code
- New SQL syntax requiring benchmark coverage
- Benchmark output format changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/sqlparser/AGENTS.md
