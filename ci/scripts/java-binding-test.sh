#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

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
buildkite-agent artifact download librisingwave_java_binding.so-"$profile" target/debug
mv target/debug/risingwave-"$profile" target/debug/risingwave
mv target/debug/risedev-dev-"$profile" target/debug/risedev-dev
mv target/debug/librisingwave_java_binding.so-"$profile" target/debug/librisingwave_java_binding.so

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-dev
chmod +x ./target/debug/librisingwave_java_binding.so

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.source.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
cargo make pre-start-dev
cargo make link-all-in-one-binaries

echo "--- starting risingwave cluster"
cargo make ci-start java-binding-demo

echo "--- Build java binding demo"
cargo make build-java-binding-java

echo "--- ingest data and run java binding"
cargo make ingest-data-and-run-java-binding

echo "--- Kill cluster"
cargo make ci-kill

echo "--- run stream chunk java binding"
cargo make run-java-binding-stream-chunk-demo
