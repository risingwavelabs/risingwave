#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

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

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Build deterministic simulation e2e test runner"
timeout 5m cargo make sslt --profile "$profile" -- --help

echo "--- deterministic simulation e2e, ci-3cn-1fe, ddl"
timeout 10s cargo make sslt --profile "$profile" -- './e2e_test/ddl/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, streaming"
timeout 5m cargo make sslt --profile "$profile" -- './e2e_test/streaming/**/*.slt'

echo "--- deterministic simulation e2e, ci-3cn-1fe, batch"
timeout 3m cargo make sslt --profile "$profile" -- './e2e_test/batch/**/*.slt'
