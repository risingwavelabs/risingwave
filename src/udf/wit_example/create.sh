#!/bin/bash

set -e

path=$(dirname "$0")
cd "$path"

if [ ! -f "./my_udf.component.wasm" ]; then
    echo "my_udf.component.wasm not found, please run ./build.sh first"
    exit 1
fi

echo "size of wasm: $(stat -f "%z" my_udf.component.wasm) bytes"
encoded=$(base64 -i my_udf.component.wasm)
echo "size of encoded wasm: ${#encoded} bytes"
# debug:   23557258
# release: 12457072

psql -h localhost -p 4566 -d dev -U root -c "DROP FUNCTION IF EXISTS count_char;"
sql="CREATE FUNCTION count_char (s varchar, c varchar) RETURNS BIGINT LANGUAGE wasm_v1 USING BASE64 '$encoded';"
echo "$sql" > create.sql
psql -h localhost -p 4566 -d dev -U root -v "ON_ERROR_STOP=1" -f ./create.sql

# test
psql -h localhost -p 4566 -d dev -U root -c "SELECT count_char('aabca', 'a');"
psql -h localhost -p 4566 -d dev -U root -c "SELECT count_char('aabca', 'b');"
psql -h localhost -p 4566 -d dev -U root -c "SELECT count_char('aabca', 'd');"
