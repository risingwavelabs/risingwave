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
  echo ">>> Installing PromQL cli client"
  # Download promql
  wget https://github.com/nalbury/promql-cli/releases/download/v0.3.0/promql-v0.3.0-linux-arm64.tar.gz
  tar -xvf promql-v0.3.0-linux-arm64.tar.gz
  chmod +x ./promql
  mv ./promql /usr/local/bin/promql
  echo ">>> Run Sanity check that PromQL is installed"
  promql --help

  echo ">>> Installing Kafka"
  wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
  tar -zxvf kafka_2.13-3.4.0.tgz

  echo ">>> Installing RisingWave components"
  ARTIFACTS="risingwave risedev-dev librisingwave_java_binding.so"
  # Create this so `risedev` tool can locate the binaries.
  mkdir -p target/release
  echo "$ARTIFACTS" | xargs -I 'buildkite-agent artifact download %-bench . && mv ./%-bench target/release/%'

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

setup_rw() {
cat <<EOF > .env
RISEDEV_CONFIGURED=true

ENABLE_PROMETHEUS_GRAFANA=true
ENABLE_ETCD=true
ENABLE_KAFKA=true
ENABLE_COMPUTE_TRACING=true
ENABLE_BUILD_RUST=true
ENABLE_RELEASE_PROFILE=true
ENABLE_ALL_IN_ONE=true
EOF
}

# Install artifacts + tools, configure environment
setup() {
  download_build_artifacts

  echo ">>> Setting up nexmark-bench"
  setup_nexmark_bench

  echo ">>> Setting up RisingWave"
  setup_rw
  echo "Success!"
}

gen_events() {
  pushd nexmark-bench
  nexmark-server -c
  NEXMARK_EVENTS=100000000
  echo "Generating "$NEXMARK_EVENTS" events"
  nexmark-server \
    --event-rate 500000 \
    --max-events "$NEXMARK_EVENTS" \
    --num-event-generators 8 1>gen_events.log 2>&1 &
  echo "Generated "$NEXMARK_EVENTS" events"
  popd
}

kafka_start() {
  nohup ./kafka_2.13-3.2.1/bin/zookeeper-server-start.sh ./opt/kafka_2.13-3.4.0/config/zookeeper.properties > zookeeper.log 2>&1 &
  nohup ./kafka_2.13-3.2.1/bin/kafka-server-start.sh ./opt/kafka_2.13-3.4.0/config/server.properties --override num.partitions=8 > kafka.log 2>&1 &
}

############## MAIN

main() {
  echo "--- Machine Debug Info"
  print_machine_debug_info

  echo "--- Running setup"
  setup

  echo "--- Starting kafka"
  kafka_start

  echo "--- Spawning nexmark events"
  # gen_events

  echo "--- Starting up RW"
  ./risedev d ci-gen-cpu-flamegraph

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

  echo "--- Cleanup"
  # TODO: cleanup s3 bucket.
  echo "Success!"
}

main