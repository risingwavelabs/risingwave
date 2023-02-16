#!/usr/bin/env bash

set -euxo pipefail

BASE_FOLDER="./src/tests/sqlsmith/tests/freeze"

generate() {
  mkdir -p "$BASE_FOLDER/$1"
  ./risedev d
  ./target/debug/sqlsmith test \
    --testdata ./src/tests/sqlsmith/tests/testdata \
    --generate "$BASE_FOLDER/$1"
}

cargo build

for i in $(seq 32)
do
  generate "$i"
done