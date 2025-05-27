#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import yaml
from pathlib import Path
from textwrap import dedent

BENCHMARK_TEMPLATE = '''
# Benchmark configuration
benchmark_name: sample_benchmark

# SQL to set up the initial schema and data (run once)
setup_sql: |
  CREATE TABLE example (
    id INTEGER PRIMARY KEY,
    name TEXT
  );

# SQL to prepare the data before each run
prepare_sql: |
  INSERT INTO example (id, name) VALUES (1, 'test'), (2, 'test2'), (3, 'test3');

# SQL to clean up after each run
conclude_sql: |
  DELETE FROM example;

# SQL to benchmark (baseline version)
baseline_sql: |
  SELECT * FROM example WHERE id > 0 ORDER BY id;

# SQL to benchmark (optimized version)
benchmark_sql: |
  SELECT * FROM example WHERE id > 0 ORDER BY id DESC;

# Number of times to run the benchmark
runs: 3
'''

def create_benchmark_script(yaml_path: Path) -> Path:
    """Create a shell script from the YAML configuration."""
    with open(yaml_path) as f:
        config = yaml.safe_load(f)

    script_content = dedent('''
        #!/usr/bin/env bash

        run_psql() {{
          if [ -n "$PSQL_URL" ]; then
            psql "$PSQL_URL" "$@"
          else
            risedev psql "$@"
          fi
        }}
        export -f run_psql

        setup() {{
          run_psql -c "{setup_sql}"
        }}

        prepare() {{
          run_psql -c "{prepare_sql}"
        }}

        conclude() {{
          run_psql -c "{conclude_sql}"
        }}

        benchmark_baseline() {{
          run_psql -c "{baseline_sql}"
        }}

        benchmark_optimized() {{
          run_psql -c "{benchmark_sql}"
        }}

        export -f setup
        export -f prepare
        export -f conclude
        export -f benchmark_baseline
        export -f benchmark_optimized

        # Run setup once
        setup

        # Run both benchmarks with hyperfine
        hyperfine --shell=bash --runs {runs} --show-output \\
          --prepare 'prepare' --conclude 'conclude' benchmark_baseline \\
          --prepare 'prepare' --conclude 'conclude' benchmark_optimized
    ''')

    # Get values from YAML, using empty strings as defaults
    setup_sql = config.get('setup_sql', '')
    prepare_sql = config.get('prepare_sql', '')
    baseline_sql = config.get('baseline_sql', '')
    benchmark_sql = config.get('benchmark_sql', '')
    conclude_sql = config.get('conclude_sql', '')
    runs = config.get('runs', 1)

    script_content = script_content.format(
        setup_sql=setup_sql,
        prepare_sql=prepare_sql,
        baseline_sql=baseline_sql,
        benchmark_sql=benchmark_sql,
        conclude_sql=conclude_sql,
        runs=runs
    )

    script_path = yaml_path.with_suffix('.sh')
    script_path.write_text(script_content)
    script_path.chmod(0o755)  # Make executable
    return script_path

def init_benchmark(bench_name: str):
    """Initialize a new benchmark with the YAML template."""
    benchmark_dir = Path("benchmarks")
    benchmark_dir.mkdir(exist_ok=True)

    benchmark_file = benchmark_dir / f"{bench_name}.yaml"
    if benchmark_file.exists():
        print(f"Error: Benchmark '{bench_name}' already exists")
        sys.exit(1)

    benchmark_file.write_text(BENCHMARK_TEMPLATE.lstrip())
    print(f"Created benchmark configuration: {benchmark_file}")

def run_benchmark(bench_name: str, profile: str = "full", pg_url: str | None = None, dump_output: bool = False):
    """Run the specified benchmark."""
    yaml_file = Path("develop/sql_bench/benchmarks") / f"{bench_name}.yaml"
    if not yaml_file.exists():
        print(f"Error: Benchmark configuration '{bench_name}' not found at {yaml_file}")
        sys.exit(1)

    # Create shell script from YAML
    script_file = create_benchmark_script(yaml_file)

    # Debug: Print script content
    if dump_output:
        print("Generated script content:")
        print(script_file.read_text())
        print()

    # Prepare environment
    env = os.environ.copy()
    if pg_url:
        env["PSQL_URL"] = pg_url

    try:
        if not pg_url:
            # Start RisingWave
            subprocess.run(
                f"ENABLE_RELEASE_PROFILE=true ./risedev d {profile}",
                shell=True,
                check=True,
                env=env
            )

        # Run benchmark
        print(f"\nRunning benchmark: {bench_name}")
        print("=" * 50)

        result = subprocess.run(
            str(script_file),
            shell=True,
            check=True,
            env=env,
            text=True,
            capture_output=True
        )

        # Always print the benchmark results
        print(result.stdout)

        # Only print errors if dump_output is True
        if dump_output and result.stderr:
            print("Errors:", file=sys.stderr)
            print(result.stderr, file=sys.stderr)

    except subprocess.CalledProcessError as e:
        print(f"Error: Benchmark '{bench_name}' failed")
        print("Output:", file=sys.stderr)
        print(e.stdout, file=sys.stderr)
        print("Errors:", file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        sys.exit(1)
    finally:
        if not pg_url:
            # Clean up RisingWave cluster and data
            subprocess.run("risedev k && risedev clean-data", shell=True, check=False)

        # Clean up the generated script
        script_file.unlink()

def main():
    parser = argparse.ArgumentParser(description="SQL Benchmark Runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Init command
    init_parser = subparsers.add_parser("init", help="Initialize a new benchmark")
    init_parser.add_argument("bench_name", help="Name of the benchmark")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run a benchmark")
    run_parser.add_argument("bench_name", help="Name of the benchmark to run")
    run_parser.add_argument("-p", "--profile", default="full",
                           help="RisingWave profile to use (default: full)")
    run_parser.add_argument("-u", "--pg-url",
                           help="PostgreSQL URL to benchmark against (bypasses RisingWave)")
    run_parser.add_argument("-d", "--dump-output", action="store_true",
                           help="Show detailed output from the benchmark runs")

    args = parser.parse_args()

    if args.command == "init":
        init_benchmark(args.bench_name)
    elif args.command == "run":
        run_benchmark(args.bench_name, args.profile, args.pg_url, args.dump_output)

if __name__ == "__main__":
    main()