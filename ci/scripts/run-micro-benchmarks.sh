#!/usr/bin/env bash

# The benchmarks here will run **per PR**, and daily in main cron.
# only add top-level microbench here. For example, executor micro-benchmarks.

set -euo pipefail

# Space delimited micro-benchmarks to run.
# Each micro-benchmark added here should return a singular result indicative of performance.
# Make sure the added benchmark has a unique name.
BENCHMARKS="stream_hash_agg json_parser bench_block_iter bench_compactor bench_lru_cache bench_merge_iter"

# Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
get_instance_type() {
  TOKEN=`curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"` \
  && curl -H "X-aws-ec2-metadata-token: $TOKEN" -v http://169.254.169.254/latest/meta-data/instance-type
}

# cargo criterion --bench stream_hash_agg --message-format=json
bench() {
  BENCHMARK_NAME=$1
  for LINE in $(cargo criterion --bench "$BENCHMARK_NAME" --message-format=json)
  do
    echo "$LINE"
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
  # FIXME(kwannoel): This is a workaround
  # Microbenchmarks need to be namespaced by instance types,
  # the result upload endpoint needs to be parameterized by instance type as well to support this.
  echo "--- Getting aws instance type"
  local instance_type=$(get_instance_type)
  echo "instance_type: $instance_type"
  echo "$instance_type" > microbench_instance_type.txt
  buildkite-agent artifact upload ./microbench_instance_type.txt
  if [[ $instance_type != "m6i.4xlarge" ]]; then
    echo "Only m6i.4xlarge is supported, skipping microbenchmark"
    exit 0
  fi

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

    OLD_IFS=$IFS
    IFS=$'\n'

    bench $BENCHMARK

    IFS=$OLD_IFS

  done


  # FIXME: the `-i` (inplace) flag doesn't work with this sed expr for some reason...
  NO_TRAILING_COMMA=$(sed -E '$ s/(.*),$/\1/' ./results.json)
  echo "$NO_TRAILING_COMMA" > ./results.json
  echo ']' >> results.json
  buildkite-agent artifact upload "./results.json"
}

local_test() {
  echo '[' > results.json
  for BENCHMARK in $BENCHMARKS
  do
    echo "--- Running $BENCHMARK"
    bench $BENCHMARK
  done
  NO_TRAILING_COMMA=$(sed -E '$ s/(.*),$/\1/' ./results.json)
  echo "$NO_TRAILING_COMMA" > ./results.json
  echo ']' >> results.json
}

set +u
if [[ "$1" == 'local' ]]; then
  set -u
  echo "Running Local microbench"
  local_test
else
  set -u
  echo "Running CI microbench"
  main
fi