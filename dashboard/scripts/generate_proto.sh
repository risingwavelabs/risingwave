#!/bin/bash

set -e

protoc --plugin=./node_modules/.bin/protoc-gen-ts_proto \
    --ts_proto_out=proto/gen/ \
    --proto_path=../proto \
    --ts_proto_opt=outputServices=false \
    ../proto/*.proto
