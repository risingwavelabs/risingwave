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

sink_test_env_setup "$profile"

export RABBITMQ_AMQP_URL="${RABBITMQ_AMQP_URL:-amqp://guest:guest@rabbitmq-server:5672/%2f}"
export RABBITMQ_MANAGEMENT_URL="${RABBITMQ_MANAGEMENT_URL:-http://rabbitmq-server:15672}"
export RABBITMQ_USERNAME="${RABBITMQ_USERNAME:-guest}"
export RABBITMQ_PASSWORD="${RABBITMQ_PASSWORD:-guest}"
export RABBITMQ_VIRTUAL_HOST="${RABBITMQ_VIRTUAL_HOST:-/}"

echo "--- testing RabbitMQ sink"
risedev slt './e2e_test/sink/rabbitmq_sink.slt'

echo "--- Kill cluster"
risedev ci-kill
