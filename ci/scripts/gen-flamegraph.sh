#!/usr/bin/env bash

# NOTE(kwannoel): When developing this script, you may want to disable the `-e` flag.
# The feedback loop is too long otherwise.
set -euo pipefail

QUERY_DIR="/risingwave/ci/scripts/sql/nexmark"

# TODO(kwannoel): This is a workaround since workdir is `/risingwave` in the docker container.
# Perhaps we should have a new docker container just for benchmarking?
pushd ..

############## JOB METADATA

# Buildkite does not support labels at the moment. Have to get via github api.
get_nexmark_queries_to_run() {
  set +u
  if [[ -z "$NEXMARK_QUERIES" ]]; then
    # every PR is an issue, we can use github api to pull it.
    echo "PULL_REQUEST: $PULL_REQUEST"
    export NEXMARK_QUERIES=$(curl -L \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer $GITHUB_TOKEN"\
      -H "X-GitHub-Api-Version: 2022-11-28" \
      https://api.github.com/repos/risingwavelabs/risingwave/issues/"$PULL_REQUEST"/labels \
    | parse_labels)
  elif [[ "$NEXMARK_QUERIES" == "all" ]]; then
    export NEXMARK_QUERIES="$(ls $QUERY_DIR | sed -n 's/^q\([0-9]*\)\.sql/\1/p' | sort -n | sed 's/\(.*\)/nexmark-q\1/')"
  else
    echo "NEXMARK_QUERIES already set."
  fi
  set -u
  echo "Nexmark queries to run: $NEXMARK_QUERIES"
}

# Meant to be piped into.
parse_labels() {
  jq ".[] | .name"  \
  | grep "nexmark-q" \
  | tr "\n" " " \
  | xargs echo -n
}

############## DEBUG INFO

print_machine_debug_info() {
  echo "Shell:"
  echo "$SHELL"
  echo "Free space on Machine:"
  df -h
}

############## INSTALL

# NOTE(kwannoel): Unused for now, maybe used if we use s3 as storage backend.
install_aws_cli() {
  echo ">>> Install aws cli"
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip -q awscliv2.zip && ./aws/install && mv /usr/local/bin/aws /bin/aws
}

# NOTE(kwannoel) we can mirror the artifacts here in an s3 bucket if there are errors with access.
install_all() {
  # jq used to parse nexmark labels.
  echo ">>> Installing jq"
  wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
  chmod +x jq-linux64
  mv jq-linux64 /usr/local/bin/jq

  # flamegraph.pl used to generate heap flamegraph
  echo ">>> Installing flamegraph.pl"
  wget https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl
  chmod +x ./flamegraph.pl

  # faster addr2line to speed up heap flamegraph analysis by jeprof
  echo ">>> Installing addr2line"
  git clone https://github.com/gimli-rs/addr2line
  pushd addr2line
  cargo b --examples -r
  mv ./target/release/examples/addr2line $(which addr2line)
  popd
  rm -rf addr2line

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
  NEXMARK_EVENTS=$((500 * 1000 * 1000))
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

gen_heap_flamegraph() {
  pushd risingwave
  set +e
  echo ">>> Heap files"
  ls -lac | grep "\.heap"
  LATEST_HEAP_PROFILE="$(ls -c | grep "\.heap" | tail -1)"
  if [[ -z "$LATEST_HEAP_PROFILE" ]]; then
    echo "No heap profile generated. Less than 4GB allocated."
    popd
    set -e
    return 1
  else
    JEPROF=$(find . -name 'jeprof' | head -1)
    chmod +x "$JEPROF"
    COMPUTE_NODE=".risingwave/bin/risingwave/compute-node"
    $JEPROF --collapsed $COMPUTE_NODE $LATEST_HEAP_PROFILE > heap.collapsed
    ../flamegraph.pl --color=mem --countname=bytes heap.collapsed > perf.svg
    mv perf.svg ..
    popd
    set -e
    return 0
  fi
}

############## MONITORING

# TODO: promql
monitor() {
  sleep $((5 * 60))
}

stop_processes() {
  # stop rw
  pushd risingwave
  ./risedev k
  ./risedev clean-data
  popd
}

############## LIB

# Install artifacts + tools, configure environment
setup() {
  install_all
  configure_all
}

# Run benchmark for a query
run_heap_flamegraph() {
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

  # NOTE(kwannoel): This step should output profile result on every 4GB memory allocation.
  # No need for an extra profiling step.
  echo "--- Starting up RW"
  pushd risingwave
  RISEDEV_ENABLE_HEAP_PROFILE=1 ./risedev d ci-gen-cpu-flamegraph
  popd

  echo "--- Machine Debug Info After RW Start"
  print_machine_debug_info

  echo "--- Running ddl"
  psql -h localhost -p 4566 -d dev -U root -f risingwave/ci/scripts/sql/nexmark/ddl.sql

  echo "--- Running Benchmarks"
  psql -h localhost -p 4566 -d dev -U root -f "$QUERY_PATH"

  # NOTE(kwannoel): Can stub first if promql gives us issues.
  # Most nexmark queries (q4-q20) will have a runtime of 10+ min.
  # We can just let flamegraph profile 5min slice.
  # TODO(kwannoel): Use promql to monitor when throughput hits 0 with 1-minute intervals.
  echo "--- Monitoring Benchmark for 5 minutes"
  monitor

  echo "--- Benchmark finished, stopping processes"
  stop_processes

  echo "--- Generate flamegraph"
  if gen_heap_flamegraph; then
    mv perf.svg "$FLAMEGRAPH_PATH"

    echo "--- Uploading flamegraph"
    buildkite-agent artifact upload "./$FLAMEGRAPH_PATH"

    echo "--- Cleaning up heap artifacts"
    pushd risingwave
    rm *.heap
    rm heap.collapsed
    popd
  else
    echo "--- No flamegraph for $QUERY"
  fi

  echo "--- Machine Debug Info After running $QUERY"
  print_machine_debug_info

}

# Run benchmark for a query
run_cpu_flamegraph() {
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
  psql -h localhost -p 4566 -d dev -U root -f "$QUERY_PATH"

  echo "--- Start Profiling"
  start_nperf

  # NOTE(kwannoel): Can stub first if promql gives us issues.
  # Most nexmark queries (q4-q20) will have a runtime of 10+ min.
  # We can just let flamegraph profile 5min slice.
  # TODO(kwannoel): Use promql to monitor when throughput hits 0 with 1-minute intervals.
  echo "--- Monitoring Benchmark for 5 minutes"
  monitor

  echo "--- Benchmark finished, stopping processes"
  # stop profiler
  pkill nperf
  stop_processes

  echo "--- Generate flamegraph"
  gen_cpu_flamegraph
  mv perf.svg $FLAMEGRAPH_PATH

  echo "--- Uploading flamegraph"
  buildkite-agent artifact upload "./$FLAMEGRAPH_PATH"

  echo "--- Machine Debug Info After running $QUERY"
  print_machine_debug_info

}

############## MAIN

main() {
  echo "--- Machine Debug Info before Setup"
  print_machine_debug_info

  echo "--- Getting Flamegraph Type (HEAP | CPU)"
  FL_TYPE="$1"
  echo "Flamegraph type: $FL_TYPE"

  echo "--- Running setup"
  setup

  # Sets nexmark queries. For example: NEXMARK_QUERIES="nexmark-q17 nexmark-q1"
  echo "--- Getting nexmark queries to run"
  get_nexmark_queries_to_run

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
    if [[ $FL_TYPE == "cpu" ]]; then
      run_cpu_flamegraph "$QUERY"
    elif [[ $FL_TYPE == "heap" ]]; then
      run_heap_flamegraph "$QUERY"
    else
      echo "ERROR, Invalid flamegraph type: $FL_TYPE, it should be (cpu|heap)"
      exit 1
    fi
  done

  # TODO(kwannoel): `trap ERR` and upload these logs.
  #  echo "--- Uploading rw logs"
  #  pushd risingwave/.risingwave/log
  #  buildkite-agent artifact upload "./*.log"
  #  popd
}

main "$@"