#!/usr/bin/env bash

set -euo pipefail

# RiseDev waits for services to start, but the default scale monitor waits 30 seconds
# and then checks every 10 seconds. Let its initial control pass run before tests
# create actors and start DML. Keep automatic parallelism control enabled.
echo "--- Waiting 45 seconds for initial automatic parallelism control"
sleep 45
