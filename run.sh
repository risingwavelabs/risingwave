#!/usr/bin/env bash

source ../risingwave-nix/functions.sh
# RUST_LOG="risingwave_stream=trace" ./risedev d full-without-monitoring
RUST_LOG="risingwave_stream=trace" ./risedev d
# sleep 10
f queries.sql