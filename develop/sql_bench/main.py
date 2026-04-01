#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import urllib.request
import yaml
from pathlib import Path

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

# SQL to clean up everything after all runs are complete
cleanup_sql: |
  DROP TABLE IF EXISTS example;

# SQL to benchmark (baseline version)
baseline_sql: |
  SELECT * FROM example WHERE id > 0 ORDER BY id;

# SQL to benchmark (optimized version)
benchmark_sql: |
  SELECT * FROM example WHERE id > 0 ORDER BY id DESC;

# Number of times to run the benchmark
runs: 3
'''

def load_benchmark_config(yaml_path: Path) -> dict:
    """Load benchmark configuration from YAML."""
    with open(yaml_path) as f:
        config = yaml.safe_load(f) or {}
    if not isinstance(config, dict):
        raise ValueError(f"Invalid benchmark config in {yaml_path}: expected a YAML object")
    return config

def fetch_metrics_snapshot(metrics_endpoint: str, metric_names: list[str]) -> dict[str, float]:
    """Fetch a sum snapshot for the given metrics from a Prometheus text endpoint."""
    snapshot = {metric_name: 0.0 for metric_name in metric_names}
    request = urllib.request.Request(metrics_endpoint, headers={"Accept": "text/plain"})
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = response.read().decode("utf-8")

    for line in payload.splitlines():
        if not line or line.startswith("#"):
            continue
        try:
            sample, value = line.rsplit(maxsplit=1)
        except ValueError:
            continue

        metric_name = sample.split("{", 1)[0]
        if metric_name not in snapshot:
            continue

        try:
            snapshot[metric_name] += float(value)
        except ValueError:
            continue

    return snapshot

def print_metrics_delta(before: dict[str, float], after: dict[str, float]) -> None:
    """Print metrics deltas in a deterministic order."""
    print("\nKPI Metrics Delta")
    print("=" * 50)
    for metric_name in sorted(before):
        delta = after[metric_name] - before[metric_name]
        print(f"{metric_name}: before={before[metric_name]:.2f}, after={after[metric_name]:.2f}, delta={delta:.2f}")

def _escape_for_bash_double_quotes(text: str) -> str:
    """Escape text so it can be safely embedded in a bash double-quoted string."""
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")

def create_benchmark_script(yaml_path: Path, dump_output: bool = False) -> Path:
    """Create a shell script from the YAML configuration."""
    config = load_benchmark_config(yaml_path)

    # Get values from YAML, using empty strings as defaults
    setup_sql = config.get('setup_sql', '')
    prepare_sql = config.get('prepare_sql', '')
    benchmark_sql = config.get('benchmark_sql', '')
    conclude_sql = config.get('conclude_sql', '')
    cleanup_sql = config.get('cleanup_sql', '')
    runs = config.get('runs', 1)
    configured_metrics_endpoint = str(config.get("metrics_endpoint", "")).strip()
    completion = config.get("completion")

    escaped_setup_sql = _escape_for_bash_double_quotes(setup_sql)
    escaped_prepare_sql = _escape_for_bash_double_quotes(prepare_sql)
    escaped_benchmark_sql = _escape_for_bash_double_quotes(benchmark_sql)
    escaped_conclude_sql = _escape_for_bash_double_quotes(conclude_sql)
    escaped_cleanup_sql = _escape_for_bash_double_quotes(cleanup_sql)
    escaped_metrics_endpoint = _escape_for_bash_double_quotes(configured_metrics_endpoint)
    extra_exports: list[str] = []

    script_lines: list[str] = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "run_psql() {",
        "  if [ -n \"${PSQL_URL:-}\" ]; then",
        "    psql \"$PSQL_URL\" \"$@\"",
        "  else",
        "    ./risedev psql \"$@\"",
        "  fi",
        "}",
        "",
        "run_sql() {",
        "  run_psql -v ON_ERROR_STOP=1 -c \"$1\"",
        "}",
        "",
        "run_psql_data() {",
        "  # risedev prints cargo-make logs to stdout; filter them out for scalar parsing.",
        "  run_psql \"$@\" | awk '!/^\\[cargo-make\\]/ && NF { print }'",
        "}",
        "",
        f"METRICS_ENDPOINT=\"${{RW_SQL_BENCH_METRICS_ENDPOINT:-{escaped_metrics_endpoint}}}\"",
        "",
        "setup() {",
        f"  run_sql \"{escaped_setup_sql}\"",
        "}",
        "",
        "prepare() {",
        f"  run_sql \"{escaped_prepare_sql}\"",
        "}",
        "",
        "conclude() {",
        f"  run_sql \"{escaped_conclude_sql}\"",
        "}",
        "",
        "cleanup() {",
        f"  run_sql \"{escaped_cleanup_sql}\"",
        "}",
        "",
    ]

    if completion is not None:
        if not isinstance(completion, dict):
            raise ValueError("Invalid benchmark config: `completion` must be a YAML object")
        metric_name = str(completion.get("metric_name", "")).strip()
        table_id_sql = str(completion.get("table_id_sql", "")).strip()
        if not metric_name:
            raise ValueError("Invalid benchmark config: `completion.metric_name` is required")
        if not table_id_sql:
            raise ValueError("Invalid benchmark config: `completion.table_id_sql` is required")
        try:
            target_delta = int(completion.get("target_delta", 0))
            timeout_sec = int(completion.get("timeout_sec", 900))
            poll_interval_sec = float(completion.get("poll_interval_sec", 1))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid benchmark config: completion fields contain invalid numbers: {exc}")
        if target_delta <= 0:
            raise ValueError("Invalid benchmark config: `completion.target_delta` must be > 0")
        if timeout_sec <= 0:
            raise ValueError("Invalid benchmark config: `completion.timeout_sec` must be > 0")
        if poll_interval_sec <= 0:
            raise ValueError("Invalid benchmark config: `completion.poll_interval_sec` must be > 0")

        escaped_metric_name = _escape_for_bash_double_quotes(metric_name)
        script_lines.extend([
            "get_counter_sum_by_table_id() {",
            "  local metric_name=\"$1\"",
            "  local table_id=\"$2\"",
            "  local endpoint=\"$3\"",
            "  local payload",
            "  if ! payload=\"$(curl -fsS \"$endpoint\")\"; then",
            "    echo 0",
            "    return 0",
            "  fi",
            "  printf '%s\\n' \"$payload\" | awk -v metric=\"$metric_name\" -v table_id=\"$table_id\" '",
            "    $1 ~ (\"^\" metric \"\\\\{\") && index($1, \"table_id=\\\"\" table_id \"\\\"\") > 0 { sum += $2 }",
            "    END { printf \"%.0f\\n\", sum + 0 }",
            "  '",
            "}",
            "",
            "wait_for_counter_delta() {",
            "  local metric_name=\"$1\"",
            "  local table_id=\"$2\"",
            "  local baseline=\"$3\"",
            "  local target_delta=\"$4\"",
            "  local timeout_sec=\"$5\"",
            "  local poll_interval_sec=\"$6\"",
            "  local endpoint=\"$7\"",
            "",
            "  local start_ts",
            "  start_ts=\"$(date +%s)\"",
            "  while true; do",
            "    local current",
            "    current=\"$(get_counter_sum_by_table_id \"$metric_name\" \"$table_id\" \"$endpoint\")\"",
            "    if [ \"$current\" -lt \"$baseline\" ]; then",
            "      baseline=\"$current\"",
            "    fi",
            "    local delta=$((current - baseline))",
            "    if [ \"$delta\" -ge \"$target_delta\" ]; then",
            "      echo \"Completion reached: metric=$metric_name table_id=$table_id delta=$delta target=$target_delta\"",
            "      break",
            "    fi",
            "",
            "    local now",
            "    now=\"$(date +%s)\"",
            "    local elapsed=$((now - start_ts))",
            "    if [ \"$elapsed\" -ge \"$timeout_sec\" ]; then",
            "      echo \"Error: timeout waiting for completion metric. metric=$metric_name table_id=$table_id delta=$delta target=$target_delta timeout_sec=$timeout_sec\" >&2",
            "      return 1",
            "    fi",
            "    sleep \"$poll_interval_sec\"",
            "  done",
            "}",
            "",
            "benchmark() {",
            "  if [ -z \"$METRICS_ENDPOINT\" ]; then",
            "    echo \"Error: completion metric is configured but metrics endpoint is empty. Set metrics_endpoint in YAML or RW_SQL_BENCH_METRICS_ENDPOINT.\" >&2",
            "    return 1",
            "  fi",
            "  local table_id",
            "  local table_id_sql",
            "  table_id_sql=$(cat <<'RW_SQL'",
            table_id_sql,
            "RW_SQL",
            ")",
            "  table_id=\"$(run_psql_data -tA -c \"$table_id_sql\" | tail -n 1 | tr -d '[:space:]')\"",
            "  if [ -z \"$table_id\" ]; then",
            "    echo \"Error: failed to resolve table_id for completion metric\" >&2",
            "    return 1",
            "  fi",
            f"  run_sql \"{escaped_benchmark_sql}\"",
            "  local baseline",
            f"  baseline=\"$(get_counter_sum_by_table_id \"{escaped_metric_name}\" \"$table_id\" \"$METRICS_ENDPOINT\")\"",
            f"  wait_for_counter_delta \"{escaped_metric_name}\" \"$table_id\" \"$baseline\" \"{target_delta}\" \"{timeout_sec}\" \"{poll_interval_sec}\" \"$METRICS_ENDPOINT\"",
            "}",
            "",
        ])
        extra_exports.extend([
            "export -f get_counter_sum_by_table_id",
            "export -f wait_for_counter_delta",
            "export METRICS_ENDPOINT",
        ])
    else:
        script_lines.extend([
            "benchmark() {",
            f"  run_sql \"{escaped_benchmark_sql}\"",
            "}",
            "",
        ])

    script_lines.extend([
        "export -f run_psql",
        "export -f run_psql_data",
        "export -f run_sql",
        "export -f setup",
        "export -f prepare",
        "export -f conclude",
        "export -f cleanup",
        "export -f benchmark",
        *extra_exports,
        "",
        "# Run setup once",
        "setup",
        "",
        "# Trap to ensure cleanup runs on script exit",
        "trap cleanup EXIT",
        "",
        "# Run benchmark with hyperfine",
        f"hyperfine --shell=bash --runs {runs}{' --show-output' if dump_output else ''} \\",
        "  --prepare 'prepare' --cleanup 'conclude' benchmark",
        "",
    ])

    script_content = "\n".join(script_lines)

    script_path = yaml_path.with_suffix('.sh')
    script_path.write_text(script_content)
    script_path.chmod(0o755)  # Make executable
    return script_path

def init_benchmark(bench_name: str):
    """Initialize a new benchmark with the YAML template."""
    benchmark_dir = Path("develop/sql_bench/benchmarks")
    benchmark_dir.mkdir(exist_ok=True)

    benchmark_file = benchmark_dir / f"{bench_name}.yaml"
    if benchmark_file.exists():
        print(f"Error: Benchmark '{bench_name}' already exists")
        sys.exit(1)

    benchmark_file.write_text(BENCHMARK_TEMPLATE.lstrip())
    print(f"Created benchmark configuration: {benchmark_file}")

def run_benchmark(bench_name: str, profile: str = "full", pg_url: str | None = None, dump_output: bool = False):
    """Run a benchmark."""
    benchmark_dir = Path("develop/sql_bench/benchmarks")
    yaml_file = benchmark_dir / f"{bench_name}.yaml"

    if not yaml_file.exists():
        print(f"Error: Benchmark configuration '{bench_name}' not found at {yaml_file}")
        sys.exit(1)

    config = load_benchmark_config(yaml_file)
    configured_metrics_endpoint = str(config.get("metrics_endpoint", "")).strip()
    metrics_endpoint = os.getenv("RW_SQL_BENCH_METRICS_ENDPOINT", configured_metrics_endpoint).strip()
    configured_kpi_metrics = config.get("kpi_metrics", [])
    if configured_kpi_metrics and not isinstance(configured_kpi_metrics, list):
        print("Error: `kpi_metrics` must be a list of metric names in benchmark YAML")
        sys.exit(1)
    kpi_metrics = [
        metric_name.strip()
        for metric_name in configured_kpi_metrics
        if isinstance(metric_name, str) and metric_name.strip()
    ]

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

        # Create and run the benchmark script
        script_file = create_benchmark_script(yaml_file, dump_output)

        try:
            metrics_before = None
            if metrics_endpoint and kpi_metrics:
                try:
                    metrics_before = fetch_metrics_snapshot(metrics_endpoint, kpi_metrics)
                    print(f"Collected KPI baseline from {metrics_endpoint}")
                except Exception as e:
                    print(
                        f"Warning: failed to collect KPI baseline from {metrics_endpoint}: {e}. "
                        "Continuing without KPI deltas.",
                        file=sys.stderr,
                    )

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

            if metrics_before is not None:
                try:
                    metrics_after = fetch_metrics_snapshot(metrics_endpoint, kpi_metrics)
                    print_metrics_delta(metrics_before, metrics_after)
                except Exception as e:
                    print(f"Warning: failed to collect KPI post-run snapshot: {e}", file=sys.stderr)

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
            # Clean up the generated script
            script_file.unlink()

    finally:
        if not pg_url:
            # Clean up RisingWave cluster and data
            subprocess.run("./risedev k && ./risedev clean-data", shell=True, check=False)

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
                           help="Show detailed output from the benchmark run")

    args = parser.parse_args()

    if args.command == "init":
        init_benchmark(args.bench_name)
    elif args.command == "run":
        run_benchmark(args.bench_name, args.profile, args.pg_url, args.dump_output)

if __name__ == "__main__":
    main()
