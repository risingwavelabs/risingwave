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

download_and_prepare_rw "$profile" source
install_e2e_test_requirements

echo "--- Setup HashiCorp Vault for testing"
export VAULT_ADDR="http://vault-server:8200"
export VAULT_TOKEN="root-token"
./ci/scripts/setup-vault.sh

echo "--- e2e, generic source test"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_meta=info" \
risedev ci-start ci-1cn-1fe-with-recovery

echo "--- Run generic source tests"
risedev slt './e2e_test/source_inline/fs/posix_fs.slt'
risedev slt './e2e_test/source_inline/refresh/refresh_table.slt'
risedev slt './e2e_test/source_inline/vault/vault_secret_ddl.slt'

echo "--- Run webhook source tests"
sleep 5
risedev slt 'e2e_test/webhook/webhook_source.slt'

risedev kill
risedev dev ci-1cn-1fe-with-recovery
sleep 20
risedev slt 'e2e_test/webhook/webhook_source_recovery.slt'

echo "--- Kill cluster"
risedev ci-kill
