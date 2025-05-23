# Local e2e SQL Benchmarks

This folder contains SQL benchmarks which are ran with [`hyperfine`](https://github.com/sharkdp/hyperfine).
We can provide a template that wraps the `hyperfine` command,
and runs it as a bash script.

It's a little premature to design a full benchmark suite for now.

You can provision a cloud instance and run the benchmarks there,
or run them locally.

**NOTE(kwannoel): These benchmarks are not ran in CI.**

## Setup a new benchmark

```bash
cp template.sh <benchmark>.sh
```

## Run a benchmark

```bash
PSQL_URL=<...>
# Run the benchmark
./<benchmark>.sh
```

## Debugging

- Add `--show-output` to the `hyperfine` command in the benchmark script to see the output of the command.
- Add `--runs 1` to run the benchmark command once and test that it works.