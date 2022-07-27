#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
mkdir -p target/debug
buildkite-agent artifact download risingwave-dev target/debug/
buildkite-agent artifact download risedev-playground-dev target/debug/
mv target/debug/risingwave-dev target/debug/risingwave
mv target/debug/risedev-playground-dev target/debug/risedev-playground

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-playground

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev playground"
cargo make pre-start-playground
cargo make link-all-in-one-binaries

echo "--- e2e test w/ Rust frontend - sink with mysql"
cargo make clean-data
cargo make ci-start















docker inspect mysql

wait_server() {
    # https://stackoverflow.com/a/44484835/5242660
    # Licensed by https://creativecommons.org/licenses/by-sa/3.0/
    {
        failed_times=0
        while ! echo -n >/dev/tcp/localhost/"$1"; do
            sleep 0.5
            failed_times=$((failed_times + 1))
            if [ $failed_times -gt 30 ]; then
                echo "ERROR: failed to start server $1 [timeout=15s]"
                exit 1
            fi
        done
    } 2>/dev/null
}

echo "Waiting for mysql sink"
wait_server 23306

echo "Waiting for cluster"
sleep 10

echo "end of prepare sink so far"
