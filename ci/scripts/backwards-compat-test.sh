#!/usr/bin/env bash

################################### SCRIPT BOILERPLATE

set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

# profile is either ci-dev or ci-release
if [[ "$profile" != "ci-dev" ]] && [[ "$profile" != "ci-release" ]]; then
    echo "Invalid option: profile must be either ci-dev or ci-release" 1>&2
    exit 1
fi

source backwards-compat-tests/scripts/utils.sh

################################### ENVIRONMENT VARIABLES

LOG_DIR=.risingwave/log
mkdir -p "$LOG_DIR"

QUERY_LOG_FILE="$LOG_DIR/query.log"

# TODO(kwannoel): automatically derive this by:
# 1. Fetching major version.
# 2. Find the earliest minor version of that major version.
TAG=1.0.0
# Duration to wait for recovery (seconds)
RECOVERY_DURATION=20

echo "--- Configuring RW"
configure_rw

echo "--- Build risedev for $TAG, it may not be backwards compatible"
git config --global --add safe.directory /risingwave
git checkout "${TAG}-rc"
cargo build -p risedev

echo "--- Setup old release $TAG"
pushd ..
git clone --depth 1 --branch "${TAG}-rc" "https://github.com/risingwavelabs/risingwave.git"
pushd risingwave
mkdir -p target/debug
echo "Branch:"
git branch
cp risingwave target/debug/risingwave

echo "--- Teardown any old cluster"
set +e
./risedev down
set -e

echo "--- Start cluster on tag $TAG"
git config --global --add safe.directory /risingwave
# NOTE(kwannoel): We use this config because kafka encounters errors upon cluster restart,
# If previous kafka topics and partitions were not removed.
./risedev d full-without-monitoring && rm .risingwave/log/*
pushd .risingwave/log/
buildkite-agent artifact upload "./*.log"
popd

# TODO(kwannoel): Run nexmark queries + tpch queries.
# TODO(kwannoel): Refactor this into a rust binary + test files for better maintainability.
echo "--- Running Queries Old Cluster @ $TAG"
run_sql_old_cluster

echo "--- Kill cluster on tag $TAG"
./risedev k

echo "--- Setup Risingwave @ $RW_COMMIT"
download_and_prepare_rw $profile common

echo "--- Start cluster on latest"
configure_rw
./risedev d full-without-monitoring

echo "--- Wait ${RECOVERY_DURATION}s for Recovery on Old Cluster Data"
sleep $RECOVERY_DURATION

echo "--- Running Queries New Cluster"
run_sql_new_cluster

echo "--- Sanity Checks"
echo "AFTER_1"
cat AFTER_1 | tail -n 100
echo "AFTER_2"
cat AFTER_2 | tail -n 100

echo "--- Comparing results"
assert_eq BEFORE_1 AFTER_1
assert_eq BEFORE_2 AFTER_2
assert_not_empty BEFORE_1
assert_not_empty BEFORE_2
assert_not_empty AFTER_1
assert_not_empty AFTER_2

echo "--- Running Updates and Deletes on new cluster should not fail"
run_updates_and_deletes_new_cluster