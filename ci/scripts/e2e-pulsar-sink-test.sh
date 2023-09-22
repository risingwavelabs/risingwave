#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- check pulsar"
curl http://127.0.0.1:6651/admin/v2/clusters

echo "--- testing pulsar sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/pulsar_sink.slt'

sleep 5