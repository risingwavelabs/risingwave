#!/usr/bin/env bash

set -euo pipefail

while getopts 'p:m:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        m )
            mode=$OPTARG
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

source ci/scripts/common.sh

download_and_prepare_rw "$profile" monitoring

echo "--- Starting RisingWave cluster with monitoring..."
./risedev d ci-3cn-1fe-with-monitoring

echo "--- Running monitoring tests"
./risedev slt './e2e_test/monitoring/internal_get_channel_delta_stats.slt'

echo "Stopping cluster..."
./risedev k
