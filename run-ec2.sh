#!/usr/bin/env bash

# source ../risingwave-nix/functions.sh
# RUST_LOG="risingwave_stream=trace" ./risedev d full-without-monitoring
RUST_LOG="risingwave_stream=trace" ./risedev d full
sleep 10
f queries2.sql
for i in $(seq 1 100000)
do
    psql -h localhost -p 4566 -d dev -U root -c "insert into sales values ($i, $i, $i * interval '1 hour' + timestamp '2023-01-01 00:00:01');"
done

./risedev k