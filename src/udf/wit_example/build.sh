#!/bin/bash

set -e

# usage: ./build.sh --lang [rust|go] [--rebuild]

while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
    --lang)
        lang="$2"
        shift # past argument
        shift # past value
        ;;
    --rebuild)
        rebuild="true"
        shift # past argument
        ;;
    *) # unknown option
        shift # past argument
        ;;
    esac
done

if [ -z "$lang" ]; then
    echo "Please specify --lang [rust|go]"
    exit 1
fi

if [ "$(wasm-tools -V)" != "wasm-tools 1.0.35" ]; then
    echo "wasm-tools 1.0.35 is required"
    exit 1
fi

path=$(dirname "$0")

function build_rust() {
    echo "--- Build Rust guest component"

    cd "$path/rust"

    rustup target add wasm32-wasi

    profile=release
    if [ "$profile" == "dev" ]; then
        target_dir=debug
    else
        target_dir=$profile
    fi

    cargo build --target=wasm32-wasi --profile "${profile}"
    mv ./target/wasm32-wasi/"${target_dir}"/my_udf.wasm ../my_udf.rust.wasm

    cd ..
}

function build_go() {
    echo "--- Build TinyGo guest component"
    # Note: TinyGo will rebuild the whole binary every time and it's slow.

    cd "$path/tinygo"
    go generate # generate bindings for Go
    tinygo build -target=wasi -o my_udf.go.wasm my_udf.go
    wasm-tools component embed ../../wit my_udf.go.wasm -o my_udf.go.wasm

    mv ./my_udf.go.wasm ..
    cd ..
}

# if the file "my_udf.$lang.wasm" does not exist, or --rebuild is specified, rebuild it
if [ ! -f "my_udf.$lang.wasm" ] || [ "$rebuild" == "true" ]; then
    if [ "$lang" == "rust" ]; then
        build_rust
    elif [ "$lang" == "go" ]; then
        build_go
    else
        echo "Unknown language: $lang"
        exit 1
    fi
else
    echo "my_udf.$lang.wasm exists, skip building"
fi



# (WASI adaptor) if file not found, download from
if [ ! -f wasi_snapshot_preview1.reactor.wasm ]; then
    wget https://github.com/bytecodealliance/wasmtime/releases/download/v10.0.1/wasi_snapshot_preview1.reactor.wasm
fi

echo wasm-tools component new "my_udf.$lang.wasm" -o my_udf.component.wasm
wasm-tools component new "my_udf.$lang.wasm" -o my_udf.component.wasm \
  --adapt wasi_snapshot_preview1=./wasi_snapshot_preview1.reactor.wasm
wasm-tools validate my_udf.component.wasm --features component-model
