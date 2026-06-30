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

echo "--- starting risingwave cluster with [session_init] config"
risedev ci-start ci-session-init
sleep 1

echo "--- verify session parameters were seeded from [session_init]"
sqllogictest -p 4566 -d dev './e2e_test/session_init/*.slt'

echo "--- Kill cluster"
risedev ci-kill
