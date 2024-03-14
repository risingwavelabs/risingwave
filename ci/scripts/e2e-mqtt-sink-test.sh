#!/usr/bin/env bash

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

download_and_prepare_rw "$profile" source

echo "--- starting risingwave cluster"
cargo make ci-start ci-sink-test
sleep 1

echo "--- testing mqtt sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/mqtt_sink.slt'

sleep 1

echo "--- Kill cluster"
cargo make ci-kill