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

source_test_env_setup "$profile" --risedev-profile ci-source-nats-test --need-python
risedev slt './e2e_test/source_inline/nats/**/*.slt.serial' --skip 'authentication.slt.serial'

source e2e_test/source_inline/nats/auth/credentials.env
export NATS_AUTH_MODE=nkey NATS_AUTH_URL=nats://nats-nkey:4222 NATS_AUTH_JWT_OPTION=''
risedev slt './e2e_test/source_inline/nats/authentication.slt.serial'

export NATS_AUTH_MODE=jwt NATS_AUTH_URL=nats://nats-jwt:4222
export NATS_AUTH_JWT_OPTION=", jwt = '${NATS_AUTH_JWT}'"
risedev slt './e2e_test/source_inline/nats/authentication.slt.serial'

echo "--- Kill cluster"
risedev ci-kill
