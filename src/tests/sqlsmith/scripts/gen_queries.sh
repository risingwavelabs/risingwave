#!/usr/bin/env bash

# NOTE(kwannoel): This script currently does not work locally...
# It has been adapted for CI use.

# USAGE: Script for generating queries via sqlsmith.
# These queries can be used for fuzz testing.
# Requires `$SNAPSHOT_DIR` to be set,
# that will be where queries are stored after generation.
#
# Example:
# SNAPSHOT_DIR="~/projects/sqlsmith-query-snapshots" ./gen_queries.sh

################# ENV

set -euo pipefail

export RUST_LOG="info"
export OUTDIR=$SNAPSHOT_DIR
export RW_HOME="../../../.."
export LOGDIR=".risingwave/log"
export TESTS_DIR="src/tests/sqlsmith/tests"
export TESTDATA="$TESTS_DIR/testdata"
export CRASH_MESSAGE="note: run with \`MADSIM_TEST_SEED=[0-9]*\` environment variable to reproduce this error"
export TIME_BOUND="6m"

################## COMMON

refresh() {
  cd src/tests/sqlsmith/scripts
  source gen_queries.sh
  cd -
}

echo_err() {
  echo "$@" 1>&2
}

################## EXTRACT
# TODO(kwannoel): Write tests for these

# Get reason for generation crash.
# -m1 means that grep will early exit returning exit code 141, so we just ignore it for simplicity.
get_failure_reason() {
  set +e
  cat "$1" | tac | grep -B 10000 -m1 "\[EXECUTING" | tac | tail -n+2
  set -e
}

check_if_failed() {
  grep -B 2 "$CRASH_MESSAGE" || true
}

# Extract queries from file $1, write to file $2
extract_queries() {
  local QUERIES=$(grep "\[EXECUTING .*\]: " < "$1" | sed -E 's/^.*\[EXECUTING .*\]: (.*)$/\1;/')
  local FAILED=$(check_if_failed < "$1")
  if [[ -n "$FAILED" ]]; then
    local FAIL_REASON=$(get_failure_reason "$1")
    echo_err "[WARN] Cluster crashed while generating queries. see $1 for more information."
    buildkite-agent artifact upload "$1"
    local QUERIES=$(echo -e "$QUERIES" | sed -E '$ s/(.*)/-- \1/')
  fi
  echo -e "$QUERIES" > "$2"
}

extract_ddl() {
  grep "\[EXECUTING CREATE .*\]: " | sed -E 's/^.*\[EXECUTING CREATE .*\]: (.*)$/\1;/' | $PG_FORMAT || true
}

extract_inserts() {
  grep "\[EXECUTING INSERT\]: " | sed -E 's/^.*\[EXECUTING INSERT\]: (.*)$/\1;/' || true
}

extract_updates() {
  grep "\[EXECUTING UPDATES\]: " | sed -E 's/^.*\[EXECUTING UPDATES\]: (.*)$/\1;/' || true
}

extract_last_session() {
  grep "\[EXECUTING TEST SESSION_VAR\]: " | sed -E 's/^.*\[EXECUTING TEST SESSION_VAR\]: (.*)$/\1;/' | tail -n 1 || true
}

extract_global_session() {
  grep "\[EXECUTING SET_VAR\]: " | sed -E 's/^.*\[EXECUTING SET_VAR\]: (.*)$/\1;/' || true
}

extract_failing_query() {
  grep "\[EXECUTING .*\]: " | tail -n 1 | sed -E 's/^.*\[EXECUTING .*\]: (.*)$/\1;/' | $PG_FORMAT || true
}

# Extract fail info from [`generate-*.log`] in log dir
# $1 := log file name prefix. E.g. if file is generate-XXX.log, prefix will be "generate"
extract_fail_info_from_logs() {
  LOGFILE_PREFIX="$1"
  for LOGFILENAME in $(ls "$LOGDIR" | grep "$LOGFILE_PREFIX")
  do
    LOGFILE="$LOGDIR/$LOGFILENAME"
    echo_err "[INFO] Checking $LOGFILE for bugs"
    FAILED=$(check_if_failed < "$LOGFILE")
    echo_err "[INFO] Checked $LOGFILE for bugs"
    if [[ -n "$FAILED" ]]; then
      echo_err "[WARN] $LOGFILE Encountered bug."

      REASON=$(get_failure_reason "$LOGFILE")
      echo_err "[INFO] extracted fail reason for $LOGFILE"
      SEED=$(echo "$LOGFILENAME" | sed -E "s/${LOGFILE_PREFIX}\-(.*)\.log/\1/")

      DDL=$(extract_ddl < "$LOGFILE")
      echo_err "[INFO] extracted ddl for $LOGFILE"
      GLOBAL_SESSION=$(extract_global_session < "$LOGFILE")
      echo_err "[INFO] extracted global var for $LOGFILE"
      INSERTS=$(extract_inserts < "$LOGFILE")
      UPDATES=$(extract_updates < "$LOGFILE")
      echo_err "[INFO] extracted dml for $LOGFILE"
      TEST_SESSION=$(extract_last_session < "$LOGFILE")
      echo_err "[INFO] extracted session var for $LOGFILE"
      QUERY=$(extract_failing_query < "$LOGFILE")
      echo_err "[INFO] extracted failing query for $LOGFILE"

      FAIL_DIR="$OUTDIR/failed/$SEED"
      mkdir -p "$FAIL_DIR"

      echo -e "$DDL" \
       "\n\n$GLOBAL_SESSION" \
       "\n\n$INSERTS" \
       "\n\n$UPDATES" \
       "\n\n$TEST_SESSION" \
       "\n\n$QUERY" > "$FAIL_DIR/queries.sql"
      echo_err "[INFO] WROTE FAIL QUERY to $FAIL_DIR/queries.sql"
      echo -e "$REASON" > "$FAIL_DIR/fail.log"
      echo_err "[INFO] WROTE FAIL REASON to $FAIL_DIR/fail.log"

      cp "$LOGFILE" "$FAIL_DIR/$LOGFILENAME"
    fi
  done
}

################# Generate

#
## Generate $TEST_NUM number of seeds.
## if `ENABLE_RANDOM_SEED=1`, we will generate random seeds.
#gen_seed() {
#  if [[ $ENABLE_RANDOM_SEED -eq 1 ]]; then
#    seq 1 32768 | shuf | tail -n "$TEST_NUM"
#  else
#    seq 1 32678 | tail -n "$TEST_NUM"
#  fi
#}
#

# Generate $TEST_NUM number of seeds.
# if `ENABLE_RANDOM_SEED=1`, we will generate random seeds.
# sample output:
# 1 12329
# 2 22929
# 3 22921
gen_seed() {
  for i in $(seq 1 100)
  do
    if [[ $ENABLE_RANDOM_SEED -eq 1 ]]; then
      echo "$i $RANDOM"
    else
      echo "$i $i"
    fi
  done
}

generate_one_deterministic() {
  local SEED=$RANDOM
  local SET_ID=$1
  mkdir -p "$OUTDIR/$SET_ID"
  echo "[INFO] Generating For Seed $RANDOM, Query set $SET_ID"
  MADSIM_TEST_SEED=$RANDOM timeout 3m "$MADSIM_BIN" \
    --sqlsmith 30 \
    --generate-sqlsmith-queries "$OUTDIR/$SET_ID" \
    $TESTDATA \
    1>>"$LOGDIR/generate_deterministic.stdout.log" \
    2>"$LOGDIR/generate-$SET_ID.log"
  local EXIT_CODE="$?"
  if [[ $EXIT_CODE -eq 0 ]];
  then
    echo "[INFO] Finished Generating For Seed $RANDOM, Query set $SET_ID"
    echo "[INFO] Extracting Queries For Seed $RANDOM, Query set $SET_ID"
    extract_queries "$LOGDIR/generate-$SET_ID.log" "$OUTDIR/$SET_ID/queries.sql"
    echo "[INFO] Extracted Queries For Seed $RANDOM, Query set $SET_ID."
  elif [[ $EXIT_CODE -eq 124 ]]
  then
    echo "[ERROR] Query timeout for Seed $RANDOM, Query set $SET_ID"
    buildkite-agent artifact upload "$LOGDIR/generate-$SET_ID.log"
    echo "[INFO] Uploaded failure logs: $LOGDIR/generate-$SET_ID.log"
  else
    echo "[ERROR] Query failed For Seed $RANDOM, Query set $SET_ID"
    buildkite-agent artifact upload "$LOGDIR/generate-$SET_ID.log"
    echo "[INFO] Uploaded failure logs: $LOGDIR/generate-$SET_ID.log"
  fi
}
#
## Prefer to use generate_deterministic, it is faster since
## runs with all-in-one binary.
#generate_deterministic() {
#  # Even if fails early, it should still generate some queries, do not exit script.
#  set +e
#  echo_err "[INFO] Generating"
#  echo "" > $LOGDIR/generate_deterministic.stdout.log
#
#  for i in $(seq 0 9)
#  do
#    local batch_size=10
#    local start="$((i * $batch_size + 1))"
#    local end=$((start - 1 + $batch_size))
#    echo_err "--- Generating for Queries $start - $end"
#    for SET_ID in $(seq $start $end)
#    do
#        generate_one_deterministic "$SET_ID" &
#    done
#    wait
#  done
#  echo_err "[INFO] Finished generation"
#
#  set -e
#}

# Prefer to use [`generate_deterministic`], it is faster since
# runs with all-in-one binary.
generate_deterministic() {
  # Allows us to use other functions defined in this file within `parallel`.
  . $(which env_parallel.bash)
  # Even if fails early, it should still generate some queries, do not exit script.
  set +e
  echo "" > $LOGDIR/generate_deterministic.stdout.log
  gen_seed | env_parallel --colsep ' ' --jobs 14 "
    mkdir -p $OUTDIR/{1}
    echo '[INFO] Generating For Seed {2}, Query Set {1}'
    MADSIM_TEST_SEED={2} timeout 2m $MADSIM_BIN \
      --sqlsmith 100 \
      --generate-sqlsmith-queries $OUTDIR/{1} \
      $TESTDATA \
      1>>$LOGDIR/generate_deterministic.stdout.log \
      2>$LOGDIR/generate-{1}.log;
    echo '[INFO] Finished Generating For Seed {2}, Query set {1}'
    echo '[INFO] Extracting Queries For Seed {2}, Query set {1}.'
    extract_queries $LOGDIR/generate-{1}.log $OUTDIR/{1}/queries.sql
    echo '[INFO] Extracted Queries For Seed {2}, Query set {1}.'
    "
  set -e
}

generate_sqlsmith() {
  mkdir -p "$OUTDIR/$1"
  ./risedev d
  ./target/debug/sqlsmith test \
    --testdata ./src/tests/sqlsmith/tests/testdata \
    --generate "$OUTDIR/$1"
}

############################# Checks

# Check that queries are different
check_different_queries() {
  if [[ -z $(diff "$OUTDIR/1/queries.sql" "$OUTDIR/2/queries.sql") ]]; then
    echo_err "[ERROR] Queries are the same! \
      Something went wrong in the generation process." \
      && exit 1
  fi
}

# Check that no queries are empty
check_queries_have_at_least_create_table() {
  for QUERY_FILE in "$OUTDIR"/*/queries.sql
  do
    set +e
    N_CREATE_TABLE="$(grep -c "CREATE TABLE" "$QUERY_FILE")"
    set -e
    if [[ $N_CREATE_TABLE -ge 1 ]]; then
      continue;
    else
      echo_err "[ERROR] Empty Query for $QUERY_FILE"
      cat "$QUERY_FILE"
      exit 1
    fi
  done
}

# Check if any query generation step failed, and any query file not generated.
check_failed_to_generate_queries() {
  if [[ "$(ls "$OUTDIR"/* | grep -c queries.sql)" -lt "$TEST_NUM" ]]; then
    echo_err "Queries not generated: "
    # FIXME(noel): This doesn't list the files which failed to be generated.
    ls "$OUTDIR"/* | grep queries.sql
    exit 1
  fi
}

# Run it to make sure it matches our expected timing for e2e test.
# Otherwise don't update this batch of queries yet.
run_queries_timed() {
  echo "" > $LOGDIR/run_deterministic.stdout.log
  timeout "$TIME_BOUND" seq 64 | parallel "MADSIM_TEST_SEED={} \
    $MADSIM_BIN --run-sqlsmith-queries $OUTDIR/{} \
      1>>$LOGDIR/run_deterministic.stdout.log \
      2>$LOGDIR/fuzzing-{}.log \
      && rm $LOGDIR/fuzzing-{}.log"
}

# Run it to make sure it should have no errors
run_queries() {
  echo "" > $LOGDIR/run_deterministic.stdout.log
  seq $TEST_NUM | parallel "MADSIM_TEST_SEED={} \
    $MADSIM_BIN --run-sqlsmith-queries $OUTDIR/{} \
      1>>$LOGDIR/run_deterministic.stdout.log \
      2>$LOGDIR/fuzzing-{}.log \
      && rm $LOGDIR/fuzzing-{}.log"
}

# Generated query sets should not fail.
check_failed_to_run_queries() {
  FAILED_LOGS=$(ls "$LOGDIR" | grep fuzzing || true)
  if [[ -n "$FAILED_LOGS" ]]; then
    echo_err -e "FAILING_LOGS: $FAILED_LOGS" && exit 1
  fi
}

################### TOP LEVEL INTERFACE

setup() {
  echo "--- Installing pg_format"
  wget https://github.com/darold/pgFormatter/archive/refs/tags/v5.5.tar.gz
  tar -xvf pgFormatter-5.5.tar.gz
  export PG_FORMAT="$PWD/pgFormatter-5.5/pg_format"

  echo "--- Configuring Test variables"
  if [[ -z "$TEST_NUM" ]]; then
    echo "TEST_NUM unset, default to TEST_NUM=100"
    TEST_NUM=100
  fi
  if [[ -z "$ENABLE_RANDOM_SEED" ]]; then
    echo "ENABLE_RANDOM_SEED unset, default ENABLE_RANDOM_SEED=false (0)"
    ENABLE_RANDOM_SEED=0
  fi
  echo "[INFO]: TEST_NUM=$TEST_NUM"
  echo "[INFO]: ENABLE_RANDOM_SEED=$ENABLE_RANDOM_SEED"
  pushd $RW_HOME
  mkdir -p $LOGDIR
}

setup_madsim() {
  download-and-decompress-artifact risingwave_simulation .
  chmod +x ./risingwave_simulation
  export MADSIM_BIN="$PWD/risingwave_simulation"
}

build() {
  setup_madsim
  echo_err "[INFO] Finished setting up madsim"
}

generate() {
  generate_deterministic
}

validate() {
  check_different_queries
  echo_err "[CHECK PASSED] Generated queries should be different"
  check_failed_to_generate_queries
  echo_err "[CHECK PASSED] No seeds failed to generate queries"
  check_queries_have_at_least_create_table
  echo_err "[CHECK PASSED] All queries at least have CREATE TABLE"
  extract_fail_info_from_logs "generate"
  echo_err "[INFO] Recorded new bugs from  generated queries"
  run_queries
  echo_err "[INFO] Queries were ran and passed"
  echo "--- Running timeout check"
  run_queries_timed
  echo_err "[INFO] pre-generated queries running in e2e deterministic test are ran and passed in $TIME_BOUND"
  echo "--- Check fail to run queries"
  check_failed_to_run_queries
  echo_err "[CHECK PASSED] Queries all ran without failure"
  echo_err "[INFO] Passed checks"
}

# sync step
# Some queries maybe be added
sync_queries() {
  pushd $OUTDIR
  git stash
  git checkout main
  git pull
  set +e
  git branch -D old-main
  set -e
  git checkout -b old-main
  git push -f --set-upstream origin old-main
  git checkout -
  popd
}

sync() {
  echo_err "[INFO] Syncing"
  sync_queries
  echo_err "[INFO] Synced"
}

# Upload step
upload_queries() {
  git config --global user.email "buildkite-ci@risingwave-labs.com"
  git config --global user.name "Buildkite CI"
  set +x
  pushd "$OUTDIR"
  git add .
  git commit -m 'update queries'
  git push origin main
  popd
  set -x
}

upload() {
  echo_err "[INFO] Uploading Queries"
  upload_queries
  echo_err "[INFO] Uploaded"
}

cleanup() {
  popd
  echo_err "[INFO] Success!"
}

################### ENTRY POINTS

run_generate() {
  echo "--- Running setup"
  setup

  echo "--- Running build"
  build

  echo "--- Running synchronizing with upstream snapshot"
  sync

  echo "--- Generating"
  generate

  echo "--- Validating"
  validate

  echo "--- Uploading"
  upload

  echo "--- Cleanup"
  cleanup
}

run_extract() {
  LOGDIR="$PWD" OUTDIR="$PWD" extract_fail_info_from_logs "fuzzing"
  for QUERY_FOLDER in failed/*
  do
    QUERY_FILE="$QUERY_FOLDER/queries.sql"
    cargo build --bin sqlsmith-reducer
    REDUCER=$RW_HOME/target/debug/sqlsmith-reducer
    if [[ $($REDUCER --input-file "$QUERY_FILE" --output-file "$QUERY_FOLDER") -eq 0 ]]; then
      echo "[INFO] REDUCED QUERY: $PWD/$QUERY_FILE"
      echo "[INFO] WROTE TO DIR: $PWD/$QUERY_FOLDER"
    else
      echo "[INFO] FAILED TO REDUCE QUERY: $QUERY_FILE"
    fi
  done
}

main() {
  if [[ $1 == "extract" ]]; then
    echo "[INFO] Extracting queries"
    run_extract
  elif [[ $1 == "generate" ]]; then
    run_generate
  else
    echo "
================================================================
 Extract / Generate Sqlsmith queries
================================================================
 SYNOPSIS
    ./gen_queries.sh [COMMANDS]

 DESCRIPTION
    This script can extract sqlsmith queries from failing logs.
    It can also generate sqlsmith queries and store them in \$SNAPSHOT_DIR.

    You should be in \`risingwave/src/tests/sqlsmith/scripts\`
    when executing this script.

    (@kwannoel: Although eventually this should be integrated into risedev)

 COMMANDS
    generate                      Expects \$SNAPSHOT_DIR to be set.
    extract                       Extracts failing query from logs.
                                  E.g. fuzzing-66.log

 EXAMPLES
    # Generate queries
    SNAPSHOT_DIR=~/projects/sqlsmith-query-snapshots ./gen_queries.sh generate

    # Extract queries from log
    ./gen_queries.sh extract
"
  fi
}

main "$1"