#!/usr/bin/env bash

set -euo pipefail

source ci/scripts/common.sh

profile=""

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
            exit 1
            ;;
    esac
done
shift $((OPTIND - 1))

if [[ -z "${profile}" ]]; then
    echo "profile is required: -p ci-dev|ci-release" 1>&2
    exit 1
fi

source_test_env_setup "$profile" --risedev-profile ci-kafka-oidc-test --need-python

export KAFKA_OIDC_BOOTSTRAP_SERVERS="message_queue_oidc:9092"
export KAFKA_OIDC_ISSUER_URL="http://keycloak_oidc:8080/realms/demo"
export KAFKA_OIDC_TOKEN_ENDPOINT_URL="${KAFKA_OIDC_ISSUER_URL}/protocol/openid-connect/token"
export KAFKA_OIDC_PRODUCER_CLIENT_ID="kafka-producer-client"
export KAFKA_OIDC_PRODUCER_CLIENT_SECRET="kafka-producer-client-secret"
export KAFKA_OIDC_CONSUMER_CLIENT_ID="kafka-consumer-client"
export KAFKA_OIDC_CONSUMER_CLIENT_SECRET="kafka-consumer-client-secret"

wait_for_command() {
    local message="$1"
    shift

    echo "--- ${message}"
    for _ in $(seq 1 60); do
        if "$@"; then
            return
        fi
        sleep 2
    done

    echo "Timed out: ${message}" 1>&2
    exit 1
}

wait_for_command "Wait for Keycloak OIDC metadata" \
    curl -fsS "${KAFKA_OIDC_ISSUER_URL}/.well-known/openid-configuration" -o /dev/null

wait_for_command "Wait for Kafka OIDC broker" \
    python3 e2e_test/kafka-oidc/kafka_oidc_admin.py metadata

echo "--- Run Kafka OIDC tests"
risedev slt './e2e_test/kafka-oidc/**/*.slt' -j1

echo "--- Kill cluster"
risedev ci-kill
