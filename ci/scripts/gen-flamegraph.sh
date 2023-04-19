#!/usr/bin/env bash

set -euo pipefail

############## LIB

print_machine_debug_info() {
  echo "Free space on Machine"
  df -h
  echo "Files on Machine"
  ls
}

download_build_artifacts() {
  # Download promql
  buildkite-agent artifact download ./promql
  sudo mv ./promql /usr/local/bin/promql

  ARTIFACTS="risingwave risedev-dev librisingwave_java_binding.so"
  # Create this so `risedev` tool can locate the binaries.
  mkdir -p target/release
  echo "$ARTIFACTS" | xargs -I 'buildkite-agent artifact download ./%-bench && mv ./%-bench target/release/%'

}

install_nexmark_bench() {
  git clone https://github.com/risingwavelabs/nexmark-bench.git
  pushd nexmark-bench
}

configure_nexmark_bench() {
cat <<EOF > .env
KAFKA_HOST="localhost:9092"
BASE_TOPIC="nexmark"
AUCTION_TOPIC="nexmark-auction"
BID_TOPIC="nexmark-bid"
PERSON_TOPIC="nexmark-person"
NUM_PARTITIONS=8
# NOTE: Due to https://github.com/risingwavelabs/risingwave/issues/6747, use `SEPARATE_TOPICS=false`
SEPARATE_TOPICS=false
RUST_LOG="nexmark_server=info"
}

### DEFAULTS
# host when running locally, use kafka1:19092 when running in docker
# KAFKA_HOST="localhost:9092"
# BASE_TOPIC="nexmark-events"
# AUCTION_TOPIC="nexmark-auction"
# BID_TOPIC="nexmark-bid"
# PERSON_TOPIC="nexmark-person"
# NUM_PARTITIONS=8
# SEPARATE_TOPICS=true
# RUST_LOG="nexmark_server=info"
EOF
}

build_nexmark_bench() {
  echo "TODO"
}

setup_nexmark_bench() {
  install_nexmark_bench
  configure_nexmark_bench
  build_nexmark_bench
}

# Install artifacts + tools, configure environment
setup() {

  echo "--- Downloading build artifacts"
  download_build_artifacts

  echo "--- Tryout promql"
  promql

  echo "--- Setting up nexmark-bench"
  # setup_nexmark_bench
  echo "Success!"
}

############## MAIN

main() {
  echo "--- Machine Debug Info"
  print_machine_debug_info

  setup

  echo "--- Spawning nexmark events"
  echo "Success!"

  echo "--- Setting up RW"
  ENABLE_RW_RELEASE_PROFILE=true ./risedev d ci-gen-cpu-flamegraph

  echo "--- Running Benchmarks"
  echo "Success!"

  echo "--- Start Profiling"
  echo "Success!"

  echo "--- Monitoring Benchmark"
  echo "Success!"

  echo "--- Benchmark finished"
  echo "Success!"

  echo "--- Generate flamegraph"
  echo "Success!"
}

main