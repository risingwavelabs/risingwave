#!/usr/bin/env bash

# NOTE(kwannoel):
# Do not run this script directly, it is meant to be sourced.
# Backwards compatibility tests consist of the following parts:
#
# 1. Setup old cluster binaries.
# 2. Seed old cluster.
# 3. Setup new cluster binaries.
# 4. Run validation on new cluster.
#
# Steps 1,3 are specific to the execution environment, CI / Local.
# This script only provides utilities for 2, 4.

################################### ENVIRONMENT CONFIG

# Duration to wait for recovery (seconds)
RECOVERY_DURATION=20

# Setup test directory
TEST_DIR=.risingwave/backwards-compat-tests/
KAFKA_PATH=.risingwave/bin/kafka
mkdir -p $TEST_DIR
cp -r backwards-compat-tests/slt/* $TEST_DIR

################################### TEST UTILIIES

# Older versions of RW may not gracefully kill kafka.
# So we duplicate the definition here.
kill_cluster() {
  # Kill other components
  tmux list-windows -t risedev -F "#{window_name} #{pane_id}" \
  | grep -v 'kafka' \
  | grep -v 'zookeeper' \
  | awk '{ print $2 }' \
  | xargs -I {} tmux send-keys -t {} C-c C-d

  set +e
  if [[ -n $(tmux list-windows -t risedev | grep kafka) ]];
  then
    echo "kill kafka, wait 5s"
    $KAFKA_PATH/bin/kafka-server-stop.sh
    sleep 5

    echo "kill zookeeper, wait 5s"
    $KAFKA_PATH/bin/zookeeper-server-stop.sh
    sleep 5
    # Kill their tmux sessions
    tmux list-windows -t risedev -F "#{pane_id}" | xargs -I {} tmux send-keys -t {} C-c C-d
  fi
  set -e

  tmux kill-session -t risedev
  test $? -eq 0 || { echo "Failed to stop all RiseDev components."; exit 1; }
}

run_sql () {
  psql -h localhost -p 4566 -d dev -U root -c "$@"
}

check_version() {
  local TAG=$1
  local raw_version=$(run_sql "SELECT version();")
  echo "--- Version"
  echo "$raw_version"
  local version=$(echo $raw_version | grep -i risingwave | sed 's/^.*risingwave-\([0-9]*\.[0-9]*\.[0-9]\).*$/\1/i')
  if [[ "$version" != "$TAG" ]]; then
    echo "Version mismatch, expected $TAG, got $version"
    exit 1
  fi
}

################################### Entry Points

create_kafka_topic() {
  "$KAFKA_PATH"/bin/kafka-topics.sh \
    --create \
    --topic backwards_compat_test_kafka_source --bootstrap-server localhost:29092
}

insert_json_kafka() {
  local JSON=$1
  echo "$JSON" | "$KAFKA_PATH"/bin/kafka-console-producer.sh \
    --topic backwards_compat_test_kafka_source \
    --bootstrap-server localhost:29092
}

seed_json_kafka() {
  insert_json_kafka '{"timestamp": "2023-07-28 07:11:00", "user_id": 1, "page_id": 1, "action": "gtrgretrg"}'
  insert_json_kafka '{"timestamp": "2023-07-28 07:11:00", "user_id": 2, "page_id": 1, "action": "fsdfgerrg"}'
  insert_json_kafka '{"timestamp": "2023-07-28 07:11:00", "user_id": 3, "page_id": 1, "action": "sdfergtth"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 4, "page_id": 2, "action": "erwerhghj"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 5, "page_id": 2, "action": "kiku7ikkk"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 6, "page_id": 3, "action": "6786745ge"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 7, "page_id": 3, "action": "fgbgfnyyy"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 8, "page_id": 4, "action": "werwerwwe"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 9, "page_id": 4, "action": "yjtyjtyyy"}'
}

# Setup table and materialized view.
# Run updates and deletes on the table.
# Get the results.
# TODO: Run nexmark, tpch queries
# TODO(kwannoel): use sqllogictest.
seed_old_cluster() {
  # Caller should make sure the test env has these.
  # They are called here because the current tests
  # may not be backwards compatible, so we need to call
  # them in old cluster environment.
  cp -r e2e_test/streaming/nexmark $TEST_DIR
  cp -r e2e_test/nexmark/* $TEST_DIR/nexmark

  cp -r e2e_test/batch/tpch $TEST_DIR
  cp -r e2e_test/tpch/* $TEST_DIR/tpch

  OLD_TAG=$1
  ./risedev clean-data
  ./risedev d full-without-monitoring && rm .risingwave/log/*

  check_version "$OLD_TAG"

  echo "--- BASIC TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/seed.slt"

  echo "--- BASIC TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_original.slt"

  echo "--- NEXMARK TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/seed.slt"

  echo "--- NEXMARK TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/validate_original.slt"

  echo "--- TPCH TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/seed.slt"

  echo "--- TPCH TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/validate_original.slt"

  echo "--- KAFKA TEST: Seeding old cluster with data"
  create_kafka_topic
  seed_json_kafka
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/seed.slt"

  echo "--- KAFKA TEST: wait 5s for kafka to process data"
  sleep 5

  echo "--- KAFKA TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/validate_original.slt"

  echo "--- Killing cluster"
  kill_cluster
  echo "--- Killed cluster"
}

validate_new_cluster() {
  NEW_TAG=$1
  echo "--- Start cluster on latest"
  ./risedev d full-without-monitoring

  echo "--- Wait ${RECOVERY_DURATION}s for Recovery on Old Cluster Data"
  sleep $RECOVERY_DURATION

  check_version "$NEW_TAG"

  echo "--- BASIC TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_restart.slt"

  echo "--- NEXMARK TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/validate_restart.slt"

  echo "--- TPCH TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/validate_restart.slt"

  echo "--- KAFKA TEST: Seeding new cluster with data"
  seed_json_kafka

  echo "--- KAFKA TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/validate_restart.slt"

  kill_cluster
}