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

source_test_env_setup "$profile" --risedev-profile ci-source-pulsar-test --need-python

risedev slt './e2e_test/source_inline/pulsar/**/*.slt' -j4
risedev slt './e2e_test/source_inline/pulsar/**/*.slt.serial'

echo "--- Kill cluster"
risedev ci-kill
