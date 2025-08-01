#!/usr/bin/env bash

# Exits as soon as any line fails.
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

download_and_prepare_rw "$profile" common

echo "--- starting risingwave cluster"
risedev ci-start ci-time-travel
sleep 1

sqllogictest -p 4566 -d dev './e2e_test/time_travel/*.slt'

echo "--- verify time travel metadata"
sleep 30 # ensure another time travel version snapshot has been taken
version_snapshot_count=$(sqlite3 .risingwave/data/sqlite/metadata.db "select count(*) from hummock_time_travel_version;")
if [ "$version_snapshot_count" -le 1 ]; then
  echo "test failed: too few version_snapshot_count, actual ${version_snapshot_count}"
  exit 1
fi

echo "--- Kill cluster"
risedev ci-kill

