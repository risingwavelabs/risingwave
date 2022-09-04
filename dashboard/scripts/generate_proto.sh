#!/bin/bash

set -e

rm -rf tmp_gen
mkdir tmp_gen
cp -a ../proto/*.proto tmp_gen

# Array in proto will conflict with JavaScript's Array, so we replace it with RwArray.
sed -i "" -e "s/Array/RwArray/" "tmp_gen/data.proto" 

protoc --plugin=./node_modules/.bin/protoc-gen-ts_proto \
    --ts_proto_out=proto/gen/ \
    --proto_path=tmp_gen \
    --ts_proto_opt=outputServices=false \
    --ts_proto_opt=oneof=unions \
    tmp_gen/*.proto

rm -rf tmp_gen
