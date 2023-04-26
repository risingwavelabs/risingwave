#!/usr/bin/env bash

# NOTE(kwannoel): When developing this script, you may want to disable the `-e` flag.
# The feedback loop is too long otherwise.
set -euo pipefail

QUERY_DIR="/risingwave/ci/scripts/sql/nexmark"

# TODO(kwannoel): This is a workaround since workdir is `/risingwave` in the docker container.
# Perhaps we should have a new docker container just for benchmarking?
pushd ..


# Buildkite does not support labels at the moment. Have to get via github api.
get_nexmark_queries_to_run() {
  # TODO: Move to install step
  wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
  chmod +x jq-linux64
  mv jq-linux64 /usr/local/bin/jq

  # every PR is an issue, we can use github api to pull it.
  echo "PULL_REQUEST: $PULL_REQUEST"
  export NEXMARK_QUERIES=$(curl -L \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $GITHUB_TOKEN"\
    -H "X-GitHub-Api-Version: 2022-11-28" \
    https://api.github.com/repos/risingwavelabs/risingwave/issues/"$PULL_REQUEST"/labels \
  | parse_labels)
  echo "Nexmark queries to run: $NEXMARK_QUERIES"
}

# Meant to be piped into.
parse_labels() {
  jq ".[] | .name"  \
  | grep "nexmark-q" \
  | tr "\n" " " \
  | xargs echo -n
}

install_aws_cli() {
  echo ">>> Install aws cli"
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip -q awscliv2.zip && ./aws/install && mv /usr/local/bin/aws /bin/aws
}

############## DEBUG INFO

print_machine_debug_info() {
  echo "Shell:"
  echo "$SHELL"
  echo "Free space on Machine:"
  df -h
}

############## INSTALL

# NOTE(kwannoel) we can mirror the artifacts here in an s3 bucket if there are errors with access.
install_all() {
  echo ">>> Installing PromQL cli client"
  # Download promql
  wget https://github.com/nalbury/promql-cli/releases/download/v0.3.0/promql-v0.3.0-linux-amd64.tar.gz
  tar -xvf promql-v0.3.0-linux-amd64.tar.gz
  chmod +x ./promql
  mv ./promql /usr/local/bin/promql
  echo ">>> Run Sanity check that PromQL is installed"
  promql --version

  echo ">>> Installing Kafka"
  wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
  tar -zxvf kafka_2.13-3.4.0.tgz

  echo ">>> Installing nexmark bench"
  buildkite-agent artifact download nexmark-server /usr/local/bin
  # Can't seem to do this in build phase.
  chmod +x /usr/local/bin/nexmark-server

  # TODO(kwannoel): eventually pin a newer version of nperf. This one works good enough for now.
  echo ">>> Installing nperf"
  wget https://github.com/koute/not-perf/releases/download/0.1.1/not-perf-x86_64-unknown-linux-gnu.tgz
  tar -xvf not-perf-x86_64-unknown-linux-gnu.tgz
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
# TODO(kwannoel): Workaround this error:
# ```
# Extracting dashboard artifacts to /risingwave/.risingwave/ui
# fatal: detected dubious ownership in repository at '/risingwave'
# ```
git config --global --add safe.directory /risingwave
pushd risingwave
cat <<EOF > risedev-components.user.env
RISEDEV_CONFIGURED=true

ENABLE_MINIO=true
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

# TODO(kwannoel): Currently we receive many errors for the dynamic libraries like so:
# ```
# major/minor/inode of /usr/lib/x86_64-linux-gnu/<some_library>.so.3" doesn't match the expected value: Some(Inode ...) != Some(Inode ...)
# ```
# This has minor effect on the flamegraph, so can ignore for now.
# could it be related to profiling on Docker? Needs further investigation.
start_nperf() {
  ./nperf record -p `pidof compute-node` -o perf.data &
}

start_kafka() {
  ./kafka_2.13-3.4.0/bin/zookeeper-server-start.sh ./kafka_2.13-3.4.0/config/zookeeper.properties > zookeeper.log 2>&1 &
  ./kafka_2.13-3.4.0/bin/kafka-server-start.sh ./kafka_2.13-3.4.0/config/server.properties --override num.partitions=8 > kafka.log 2>&1 &
  echo "Should have zookeeper and kafka running"
  echo "zookeeper PID: "
  pgrep zookeeper
  echo "kafka PID: "
  pgrep kafka
  sleep 10
  # TODO(kwannoel): `trap ERR` and upload these logs.
  # buildkite-agent artifact upload ./zookeeper.log
  # buildkite-agent artifact upload ./kafka.log
}

# Currently 100mil events in Kafka take up 17% of 200GB gp3 i.e. 34GB
# 1bil records would likely exceed the current storage,
gen_events() {
  pushd nexmark-bench
  nexmark-server -c
  NEXMARK_EVENTS=$((100 * 1000 * 1000))
  echo "Generating $NEXMARK_EVENTS events"
  nexmark-server \
    --event-rate 500000 \
    --max-events "$NEXMARK_EVENTS" \
    --num-event-generators 8 1>gen_events.log 2>&1
  echo "Generated $NEXMARK_EVENTS events"
  popd
  show_kafka_topics
}

show_kafka_topics() {
  ./kafka_2.13-3.4.0/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic nexmark --bootstrap-server localhost:9092
}

gen_cpu_flamegraph() {
  ./nperf flamegraph --merge-threads perf.data > perf.svg
}

############## MONITORING

# TODO: promql
monitor() {
  sleep $((5 * 60))
}

stop_processes() {
  # stop profiler
  pkill nperf

  # stop rw
  pushd risingwave
  ./risedev k
  popd
}

############## LIB

# Install artifacts + tools, configure environment
setup() {
  install_all
  configure_all
}

# Run benchmark for a query
run() {
  echo "--- Running benchmark for $QUERY"
  echo "--- Setting variables"
  QUERY_LABEL="$1"
  QUERY_FILE_NAME="$(echo $QUERY_LABEL | sed 's/nexmark\-\(.*\)/\1.sql/')"
  QUERY_PATH="$QUERY_DIR/$QUERY_FILE_NAME"
  FLAMEGRAPH_PATH="perf-$QUERY_LABEL.svg"
  echo "QUERY_LABEL: $QUERY_LABEL"
  echo "QUERY_FILE_NAME: $QUERY_FILE_NAME"
  echo "QUERY_PATH: $QUERY_PATH"
  echo "FLAMEGRAPH_PATH: $FLAMEGRAPH_PATH"

  echo "--- Starting up RW"
  pushd risingwave
  ./risedev d ci-gen-cpu-flamegraph
  popd

  echo "--- Machine Debug Info After RW Start"
  print_machine_debug_info

  echo "--- Running ddl"
  psql -h localhost -p 4566 -d dev -U root -f risingwave/ci/scripts/sql/nexmark/ddl.sql

  echo "--- Running Benchmarks"
  # TODO(kwannoel): Allow users to configure which query they want to run.
  psql -h localhost -p 4566 -d dev -U root -f $QUERY_PATH

  echo "--- Start Profiling"
  start_nperf

  # NOTE(kwannoel): Can stub first if promql gives us issues.
  # Most nexmark queries (q4-q20) will have a runtime of 10+ min.
  # We can just let flamegraph profile 5min slice.
  # TODO(kwannoel): Use promql to monitor when throughput hits 0 with 1-minute intervals.
  echo "--- Monitoring Benchmark for 5 minutes"
  monitor

  echo "--- Benchmark finished, stopping processes"
  stop_processes

  echo "--- Generate flamegraph"
  gen_cpu_flamegraph
  mv perf.svg $FLAMEGRAPH_PATH

  echo "--- Uploading flamegraph"
  buildkite-agent artifact upload "./$FLAMEGRAPH_PATH"
}

############## MAIN

main() {
  echo "--- Machine Debug Info before Setup"
  print_machine_debug_info

  # Sets nexmark queries. For example: NEXMARK_QUERIES="nexmark-q17 nexmark-q1"
  echo "--- Getting nexmark queries to run"
  get_nexmark_queries_to_run

  echo "--- Running setup"
  setup

  echo "--- Machine Debug Info after Setup"
  print_machine_debug_info

  echo "--- Starting kafka"
  start_kafka

  echo "--- Spawning nexmark events"
  gen_events

  echo "--- Machine Debug Info After Nexmark events generated"
  print_machine_debug_info

  for QUERY in $NEXMARK_QUERIES
  do
    run "$QUERY"
  done

  # TODO(kwannoel): `trap ERR` and upload these logs.
  #  echo "--- Uploading rw logs"
  #  pushd risingwave/.risingwave/log
  #  buildkite-agent artifact upload "./*.log"
  #  popd
}

main