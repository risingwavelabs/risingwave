#!/usr/bin/env bash

set -euo pipefail

# FIXME(kwannoel): This is a workaround since workdir is `/risingwave` by default.
pushd ..

############## DEBUG INFO

print_machine_debug_info() {
  echo "Shell:"
  echo "$SHELL"
  echo "Free space on Machine:"
  df -h
  echo "Files on Machine:"
  ls -la
}

############## INSTALL

install_all() {
  echo ">>> Installing PromQL cli client"
  # Download promql
  wget https://github.com/nalbury/promql-cli/releases/download/v0.3.0/promql-v0.3.0-linux-arm64.tar.gz
  tar -xvf promql-v0.3.0-linux-arm64.tar.gz
  chmod +x ./promql
  mv ./promql /usr/local/bin/promql
  # FIXME(kwannoel): For some reason this hangs...
  # echo ">>> Run Sanity check that PromQL is installed"
  # promql --help

  echo ">>> Installing Kafka"
  wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
  tar -zxvf kafka_2.13-3.4.0.tgz

  echo ">>> Installing nexmark bench"
  buildkite-agent artifact download nexmark-server /usr/local/bin
  echo "nexmark bench should be in path: $(which nexmark-server)"

  # FIXME: pin a newer version of nperf.
  echo ">>> Installing nperf"
  wget https://github.com/koute/not-perf/releases/download/0.1.1/not-perf-x86_64-unknown-linux-gnu.tgz
  tar -xvf not-perf-x86_64-unknown-linux-gnu.tgz

  echo ">>> Installing RisingWave components (includes other components installed by risedev (prometheus + grafana + etcd)"
  pushd risingwave
  artifacts=(risingwave risedev-dev librisingwave_java_binding.so)
  # Create this so `risedev` tool can locate the binaries.
  mkdir -p target/release
  echo -n "${artifacts[*]}" | parallel -d ' ' "buildkite-agent artifact download {}-bench . && mv ./{}-bench target/release/{}"
  popd
}

############## CONFIGURE

configure_nexmark_bench() {
mkdir -p nexmark-bench
pushd nexmark-bench
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
popd
}

configure_rw() {
pushd risingwave
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
popd
}

configure_all() {
  echo ">>> Configuring rw"
  configure_rw
  echo ">>> Configuring nexmark bench"
  configure_nexmark_bench
}

############## Start benchmark environment

start_nperf() {
  ./nperf record -p $(pidof compute-node) -o perf.data
}

start_kafka() {
  nohup ./kafka_2.13-3.2.1/bin/zookeeper-server-start.sh ./opt/kafka_2.13-3.4.0/config/zookeeper.properties > zookeeper.log 2>&1 &
  nohup ./kafka_2.13-3.2.1/bin/kafka-server-start.sh ./opt/kafka_2.13-3.4.0/config/server.properties --override num.partitions=8 > kafka.log 2>&1 &
}

gen_events() {
  pushd nexmark-bench
  nexmark-server -c
  NEXMARK_EVENTS=100000000
  echo "Generating $NEXMARK_EVENTS events"
  nexmark-server \
    --event-rate 500000 \
    --max-events "$NEXMARK_EVENTS" \
    --num-event-generators 8 1>gen_events.log 2>&1 &
  echo "Generated $NEXMARK_EVENTS events"
  popd
}

gen_cpu_flamegraph() {
  ~/not-perf/target/release/nperf flamegraph --merge-threads perf.data > perf.svg
}

############## MONITORING

# TODO

############## LIB

# Install artifacts + tools, configure environment
setup() {
  install_all
  configure_all
}

############## MAIN

main() {
  echo "--- Machine Debug Info before Setup"
  print_machine_debug_info

  echo "--- Running setup"
  setup

  echo "--- Machine Debug Info after Setup"
  print_machine_debug_info

  echo "--- Starting kafka"
  start_kafka

  echo "--- Spawning nexmark events"
  gen_events

  echo "--- Starting up RW"
  pushd risingwave
  ./risedev d ci-gen-cpu-flamegraph
  popd

  echo "--- Running ddl"
  psql -h localhost -p 4566 -d dev -U root -f ci/scripts/sql/nexmark/ddl.sql

  echo "--- Running Benchmarks"
  # TODO(kwannoel): Allow users to configure which query they want to run.
  psql -h localhost -p 4566 -d dev -U root -f ci/scripts/sql/nexmark/q17.sql

  echo "--- Start Profiling"
  start_nperf

  # NOTE(kwannoel): Can stub first if promql gives us issues.
  # Most nexmark queries (q4-q20) will have a runtime of 10+ min.
  # We can just let flamegraph profile 5min slice.
  # TODO(kwannoel): Use promql to monitor when throughput hits 0 with 1-minute intervals.
  echo "--- Monitoring Benchmark"
  sleep $((5 * 60))

  echo "--- Benchmark finished"
  echo "Success!"

  # NOTE(kwannoel): We can only generate cpu flamegraph OR extract metrics.
  # Running profiling will take up 30-50% of a single CPU core.
  # So that will affect benchmark accuracy.
  # Another solution could be to:
  # 1. Run cpu flamegraph for first 10 mins.
  # 2. Monitor throughput for next 10 mins.
  # Benchmark will still not be as accurate as just running benchmark w/o profiling,
  # but quick way to get both results.
  echo "--- Generate flamegraph"
  gen_cpu_flamegraph

  echo "--- Uploading flamegraph"
  buildkite-agent artifact upload ./perf.svg

  echo "--- Cleanup"
  # TODO: cleanup s3 bucket.
  echo "Success!"
}

main