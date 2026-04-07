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

export PGPASSWORD='post\tgres'
psql -h db -U postgres -c "DROP DATABASE IF EXISTS sink_test WITH (FORCE)" || true

echo "--- testing postgres sink"
risedev slt './e2e_test/sink/postgres_sink.slt'

echo "--- Kill cluster"
risedev ci-kill
