#!/usr/bin/env bash

set -euo pipefail

RUST_LOG="info,risingwave_stream=trace" ./risedev d; ./risedev psql -f error.sql