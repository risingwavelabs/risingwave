#!/usr/bin/env bash

# Exits as soon as any line fails.
# set -euo pipefail

# source ci/scripts/common.sh

# trap "echo Received SIGINT" SIGINT
# trap "echo Received SIGTERM" SIGTERM
# trap "echo Received SIGQUIT" SIGQUIT

echo "Running as process id $$"

export PYTHONUNBUFFERED=1

echo "--- Running signal-test-sub"
ci/scripts/signal-test-sub.py
echo "--- Finished signal-test-sub"
