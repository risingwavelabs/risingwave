#!/usr/bin/env bash

set -e

rm -rf tmp_gen
mkdir tmp_gen
cp -a ../proto/*.proto tmp_gen

# Array in proto will conflict with JavaScript's Array, so we replace it with RwArray.
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i "" -e "s/Array/RwArray/" "tmp_gen/data.proto" 
else
    sed -i -e "s/Array/RwArray/" "tmp_gen/data.proto" 
fi

protoc --plugin=./node_modules/.bin/protoc-gen-ts_proto \
    --experimental_allow_proto3_optional \
    --ts_proto_out=proto/gen/ \
    --proto_path=tmp_gen \
    --ts_proto_opt=outputServices=false \
    --ts_proto_opt=oneof=unions \
    --ts_proto_opt=enumsAsLiterals=true \
    --ts_proto_opt=outputEncodeMethods=false \
    --ts_proto_opt=outputClientImpl=false \
    --ts_proto_opt=stringEnums=true \
    tmp_gen/*.proto

rm -rf tmp_gen
