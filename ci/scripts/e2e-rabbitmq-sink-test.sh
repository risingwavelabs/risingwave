#!/usr/bin/env bash

set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        * )
            echo "Usage: $0 -p <build-profile>" >&2
            exit 1
            ;;
    esac
done
shift $((OPTIND -1))

sink_test_env_setup "${profile:?build profile is required}" --risedev-profile ci-sink-rabbitmq-test

echo "--- testing RabbitMQ sink"
python3 -m unittest discover -s e2e_test/sink/rabbitmq -p 'test_*.py'
risedev slt './e2e_test/sink/rabbitmq_sink.slt'

echo "--- testing RabbitMQ sink recovery (serial)"
risedev slt './e2e_test/sink/rabbitmq_recovery.slt.serial'

echo "--- Kill cluster"
risedev ci-kill
