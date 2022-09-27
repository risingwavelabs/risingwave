# Micro Benchmark for Batch Executors

We use [criterion](https://bheisler.github.io/criterion.rs/book/index.html) micro-benchmarking tool. For more details on how to write and run benchmarks, please refer to its documentation.

## Run Benchmark

Run all benchmarks

```bash
cargo bench -p risingwave_batch
```

Run a specific benchmark

```bash
cargo bench -p risingwave_batch -- <filter>
```

where `<filter>` is a regular expression matching the benchmark ID, e.g., 
`top_n.rs` uses `BenchmarkId::new("TopNExecutor", params)` , so we can run TopN benchmarks with

```bash
# All TopN benchmarks
cargo bench -p risingwave_batch -- TopN
# One specific setting of TopN benchmarks
cargo bench -p risingwave_batch -- "TopNExecutor/2048\(single_column: true\)"
```

> *Note*: `-p risingwave_batch` can be omitted if you are in the `src/batch` directory.

## Add new Benchmarks

* Add benchmark target to `src/batch/Cargo.toml`
* Implement benchmarks in `src/batch/benches`, refering to existing ones
