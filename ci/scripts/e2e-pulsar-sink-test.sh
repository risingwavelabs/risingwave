#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# echo "--- check pulsar"
# curl http://pulsar:8080/admin/v2/clusters

echo "--- testing pulsar sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/pulsar_sink.slt'

sleep 5