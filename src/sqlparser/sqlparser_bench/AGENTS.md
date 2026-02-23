# AGENTS.md - SQL Parser Benchmarks

## 1. Scope

Directory: `src/sqlparser/sqlparser_bench`

Criterion-based benchmark suite for the RisingWave SQL parser.

## 2. Purpose

Measures SQL parsing performance to detect regressions and optimize parser throughput:
- Benchmark core parsing operations with representative SQL workloads
- Compare tokenizer and parser performance across different query types
- Track parsing latency for complex DDL and DML statements
- Provide data for parser optimization decisions

## 3. Structure

```
src/sqlparser/sqlparser_bench/
├── benches/
│   └── sqlparser_bench.rs    # Criterion benchmark definitions
├── Cargo.toml                # Benchmark dependencies and configuration
└── AGENTS.md                 # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `benches/sqlparser_bench.rs` | Criterion benchmark cases for SQL parsing |
| `Cargo.toml` | Benchmark target configuration and dependencies |

## 5. Edit Rules (Must)

- Use `criterion` for statistical benchmarking with adequate sample sizes
- Include diverse SQL workloads: simple queries, complex joins, DDL statements
- Benchmark both tokenizer and full parser pipeline separately
- Use `black_box` to prevent compiler optimizations skewing results
- Run benchmarks on consistent hardware for comparable results
- Document benchmark SQL complexity in comments
- Run `cargo fmt` before committing
- Ensure benchmarks compile: `cargo check --benches -p risingwave_sqlparser`

## 6. Forbidden Changes (Must Not)

- Remove statistical rigor (minimum sample sizes, warm-up iterations)
- Benchmark only trivial SQL (must represent real workloads)
- Hardcode file paths for test SQL
- Skip CI benchmark compilation checks
- Use `#[bench]` (deprecated) instead of criterion
- Add I/O operations inside benchmark loops
- Use non-deterministic data generation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run benchmarks | `cargo bench -p risingwave_sqlparser_bench` |
| Check benches | `cargo check --benches -p risingwave_sqlparser` |
| Benchmark specific | `cargo bench -p risingwave_sqlparser_bench -- parse_simple` |

## 8. Dependencies & Contracts

- `criterion`: Statistical benchmarking framework
- `risingwave_sqlparser`: Parser under test
- Benchmarks use static SQL strings for reproducibility
- Output saved to `target/criterion/` for analysis
- Criterion generates HTML reports automatically

## 9. Overrides

None. Inherits from parent `src/sqlparser/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New benchmark cases added
- Benchmark framework changes (criterion version)
- Parser API changes affecting benchmark code
- New SQL syntax requiring benchmark coverage

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/src/sqlparser/AGENTS.md`
