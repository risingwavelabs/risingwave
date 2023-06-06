#!/usr/bin/env bash

set -euo pipefail

rc () {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
}

# Checkout <old-version>
git checkout origin/v0.18.0-rc
set +e
./risedev down
set -e
./risedev d my-full
echo "--- Running queries"
rc "CREATE TABLE t(v1 int);"
for i in $(seq 1 10000)
do
    rc "INSERT into t values (100);"
done

rc "flush;"
rc "CREATE MATERIALIZED VIEW m as SELECT * from t;"
rc "select * from m;"

echo ""
./risedev k
git checkout main
./risedev d my-full
sleep 20
rc "SELECT * FROM t;"
rc "SELECT * from m;"