#!/usr/bin/env bash

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



echo "--- Download artifacts"
mkdir -p target/debug
download-and-decompress-artifact risingwave-"$profile" target/debug/
download-and-decompress-artifact risedev-dev-"$profile" target/debug/

mv target/debug/risingwave-"$profile" target/debug/risingwave
mv target/debug/risedev-dev-"$profile" target/debug/risedev-dev

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-dev

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
risedev pre-start-dev
risedev link-all-in-one-binaries

echo "--- starting risingwave cluster with connector node"
risedev ci-start ci-1cn-1fe

echo "--- Run test"
python3 -m pip install --break-system-packages psycopg2-binary
python3 e2e_test/source_legacy/pulsar/astra-streaming.py
# python3 e2e_test/source_legacy/pulsar/streamnative-cloud.py

echo "--- Kill cluster"
risedev ci-kill
