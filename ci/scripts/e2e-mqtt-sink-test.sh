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

sink_test_env_setup "$profile" --risedev-profile ci-sink-mqtt-test

set -a
# shellcheck source=/dev/null
source .risingwave/config/risedev-env
set +a

export MQTT_URL="tcp://mqtt-server:1883"
export RISEDEV_MQTT_URL="tcp://mqtt-server:1883"
export RISEDEV_MQTT_WITH_OPTIONS_COMMON="connector='mqtt',url='tcp://mqtt-server:1883'"

echo "--- testing mqtt sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/mqtt_sink.slt'

sleep 1

echo "--- Kill cluster"
risedev ci-kill
