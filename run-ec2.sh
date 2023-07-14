#!/usr/bin/env bash

./risedev k && ./risedev clean-data

# source ../risingwave-nix/functions.sh
# RUST_LOG="risingwave_stream=trace" ./risedev d full-without-monitoring
RUST_LOG="risingwave_stream=trace" ./risedev d full
sleep 10
psql -h localhost -p 4566 -d dev -U root -f queries2.sql
for i in $(seq 1 10000000)
do
    psql -h localhost -p 4566 -d dev -U root -c "
insert into sales values ($i, $i, $i * interval '1 hour' + timestamp '2023-01-01 00:00:01');
" &
if [[ $(( $i % 16 )) -eq 0 ]]; then
        wait
fi
done