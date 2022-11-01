#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

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

echo "--- Download artifacts"
mkdir -p target/debug
buildkite-agent artifact download risingwave-"$profile" target/debug/
buildkite-agent artifact download risedev-dev-"$profile" target/debug/
buildkite-agent artifact download "e2e_test/generated/*" ./
mv target/debug/risingwave-"$profile" target/debug/risingwave
mv target/debug/risedev-dev-"$profile" target/debug/risedev-dev

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-dev

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
cargo make pre-start-dev
cargo make link-all-in-one-binaries

echo "--- e2e, ci-compaction-test, nexmark"
cargo make clean-data
cargo make ci-start ci-compaction-test
# Please make sure the regression is expected before increasing the timeout.
sqllogictest -p 4566 -d dev './e2e_test/compaction/ingest_rows.slt'

# We should ingest about 100 version deltas before the test
echo "--- Wait for data ingestion"
# Poll the current version id until we have generated 100 version deltas
delta_log_cnt=0
while [ $delta_log_cnt -le 100 ]
do
    delta_log_cnt="$(./risedev ctl hummock list-version | grep -w '^ *id:' | grep -m1 -o '[0-9]\+')"
    sleep 1
done

echo "--- Pause source and disable commit new epochs"
./risedev ctl meta pause
./risedev ctl hummock disable-commit-epoch

echo "--- Start to run compaction test"
./risedev compaction-test --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001

echo "--- Kill cluster"
cargo make ci-kill
