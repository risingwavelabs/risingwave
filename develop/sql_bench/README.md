# SQL Benchmarks

## Background

There are a lot of features and optimizations which require benchmarking to measure their performance change. Currently there's high overhead to add performance benchmarks to our nexmark suite. However, we want a fast feedback loop to iterate on these features. The result is that we write adhoc benchmarks. These are often discarded after the feature/optimization is merged, and they may not measure the performance accurately. Further, we have to write our own benchmark scripts from scratch each time.

To solve this, we've created a simple benchmark runner which allows us to write benchmarks in YAML files.
These sql benchmarks should be lightweight, and enable us to rapidly iterate on features and optimizations.

## Overview

This folder contains SQL benchmarks which are run using [`hyperfine`](https://github.com/sharkdp/hyperfine).
Benchmarks are defined using YAML configurations and executed through a Python runner.

**NOTE: These benchmarks are not run in CI.**

## Prerequisites

- Python 3.x
- `hyperfine`
- Required Python packages: `uv pip install -r requirements.txt`

## Creating a New Benchmark

Initialize a new benchmark configuration:

```bash
python main.py init <benchmark_name>
```

This creates a YAML file in the `benchmarks/` directory with the following structure:

```yaml
# Benchmark configuration
benchmark_name: example

# SQL to set up the initial schema and data (run once)
setup_sql: |
  CREATE TABLE example (...);

# SQL to prepare the data before each run
prepare_sql: |
  INSERT INTO example ...;

# SQL to clean up after each run
conclude_sql: |
  DELETE FROM example;

# SQL to benchmark (baseline version)
baseline_sql: |
  SELECT * FROM example ...;

# SQL to benchmark (optimized version)
benchmark_sql: |
  SELECT * FROM example ...;

# Number of times to run the benchmark
runs: 3
```

## Running Benchmarks

Run a benchmark using either:

```bash
# Against a local RisingWave instance
python main.py run <benchmark_name>

# Against a specific PostgreSQL URL
python main.py run <benchmark_name> -u <postgres_url>

# Show detailed output
python main.py run <benchmark_name> -d

# Use a specific RisingWave profile
python main.py run <benchmark_name> -p <profile>
```

## Debugging

- Use the `-d` or `--dump-output` flag to see detailed output from the benchmark runs
- Set `runs: 1` in the YAML config to quickly test if a benchmark works
- Check the generated shell script by running with `-d`

## Directory Structure

```
sql_bench/
├── README.md
├── main.py           # Python benchmark runner
├── requirements.txt  # Python dependencies
└── benchmarks/      # YAML benchmark configurations
    ├── example.yaml
    └── ...
```