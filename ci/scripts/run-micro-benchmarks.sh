#!/usr/bin/env bash

# The benchmarks here will run **per PR**, and daily in main cron.
# only add top-level microbench here. For example, executor micro-benchmarks.

set -euo pipefail

# Space delimited micro-benchmarks to run.
# Each micro-benchmark added here should return a singular result indicative of performance.
# Make sure the added benchmark has a unique name.
BENCHMARKS="stream_hash_agg json_parser"

# cargo criterion --bench stream_hash_agg --message-format=json
bench() {
  BENCHMARK_NAME=$1
  for LINE in $(cargo criterion --bench "$BENCHMARK_NAME" --message-format=json)
  do
    REASON="$(jq ".reason" <<< "$LINE")"
    if [[ $REASON == \"benchmark-complete\" ]]; then
      ID="$(jq ".id" <<< "$LINE")"
      MEAN="$(jq ".mean" <<< "$LINE")"
      EST="$(jq ".estimate" <<< $MEAN)"
      UNIT="$(jq ".unit" <<< $MEAN)"

      echo "Benchmark ID: $ID"
      echo "Average Time Taken: $EST"

      JSON="  {\"benchmark_id\": $ID, \"time_taken\": $EST, \"unit\": $UNIT},"
      echo -n "Json output: "
      echo "$JSON" | tee -a results.json
    fi
  done
}

main() {
  # We need cargo criterion to generate machine-readable benchmark results from
  # microbench.
  echo "--- Installing cargo criterion"
  cargo install --version "1.1.0" cargo-criterion

  echo ">>> Installing jq"
  wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
  chmod +x jq-linux64
  mv jq-linux64 /usr/local/bin/jq

  echo '[' > results.json
  for BENCHMARK in $BENCHMARKS
  do
    echo "--- Running $BENCHMARK"
    bench $BENCHMARK
  done
  # FIXME: the `-i` (inplace) flag doesn't work with this sed expr for some reason...
  NO_TRAILING_COMMA=$(sed -E '$ s/(.*),$/\1/' ./results.json)
  echo "$NO_TRAILING_COMMA" > ./results.json
  echo ']' >> results.json
  buildkite-agent artifact upload "./results.json"
}

main