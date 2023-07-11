#!/usr/bin/env bash

source ../risingwave-nix/functions.sh
# RUST_LOG="risingwave_stream=trace" ./risedev d full-without-monitoring
RUST_LOG="risingwave_stream=trace" ./risedev d full
# sleep 10
f queries2.sql
./risedev k