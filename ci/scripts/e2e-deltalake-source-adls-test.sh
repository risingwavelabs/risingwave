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

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        echo "missing required environment variable: ${name}" >&2
        exit 1
    fi
}

require_env RISEDEV_DELTALAKE_ADLS_LOCATION
require_env RISEDEV_DELTALAKE_ADLS_CLIENT_ID
require_env RISEDEV_DELTALAKE_ADLS_CLIENT_SECRET
require_env RISEDEV_DELTALAKE_ADLS_TENANT_ID

download_and_prepare_rw "$profile" source

echo "--- e2e, Delta Lake source on Azure ADLS"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_meta=info" \
risedev ci-start ci-1cn-1fe-with-recovery

echo "--- Run Delta Lake ADLS source test"
risedev slt './e2e_test/source_inline/deltalake/adls.slt'

echo "--- Kill cluster"
risedev ci-kill
