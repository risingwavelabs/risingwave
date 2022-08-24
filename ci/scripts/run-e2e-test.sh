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

echo "--- e2e, ci-3cn-1fe, streaming"
cargo make ci-start ci-3cn-1fe
# Please make sure the regression is expected before increasing the timeout.
timeout 3m sqllogictest -p 4566 -d dev './e2e_test/streaming/**/*.slt' --junit "streaming-${profile}"

echo "--- Kill cluster"
cargo make ci-kill

echo "--- e2e, ci-3cn-1fe, batch distributed"
cargo make ci-start ci-3cn-1fe
timeout 2m sqllogictest -p 4566 -d dev './e2e_test/ddl/**/*.slt' --junit "batch-ddl-${profile}"
timeout 3m sqllogictest -p 4566 -d dev './e2e_test/batch/**/*.slt' --junit "batch-${profile}"
timeout 2m sqllogictest -p 4566 -d dev './e2e_test/database/prepare.slt'
timeout 2m sqllogictest -p 4566 -d test './e2e_test/database/test.slt'

echo "--- Kill cluster"
cargo make ci-kill

echo "--- e2e, ci-3cn-1fe, extended query"
cargo make ci-start ci-3cn-1fe
timeout 2m sqllogictest -p 4566 -d dev -e postgres-extended './e2e_test/extended_query/**/*.slt'

echo "--- Kill cluster"
cargo make ci-kill
