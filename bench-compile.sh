#!/bin/bash


# default
cmd_default='RUSTFLAGS="--cfg tokio_unstable" cargo build -p risingwave_cmd_all --timings'

# parallel frontend
cmd_pf='RUSTFLAGS="--cfg tokio_unstable -Z threads=8" cargo build -p risingwave_cmd_all --timings'

# incremental build
hyperfine -r3 -p  'cargo build -p risingwave_cmd_all && echo >> src/common/src/lib.rs' \
    "$cmd_default" \
    "$cmd_pf"

# full build
# hyperfine -r3 -p 'cargo clean' \
#     "$cmd_default" \
#     "$cmd_pf"

# cranelift: aws-lc-rs link failure
# rustup component add rustc-codegen-cranelift-preview
# RUSTFLAGS="--cfg tokio_unstable" CARGO_PROFILE_DEV_CODEGEN_BACKEND=cranelift \
# cargo build -Zcodegen-backend $FLAGS --target-dir=target/cranelift

# # parallel frontend + cranelift
# RUSTFLAGS="--cfg tokio_unstable -Z threads=8" CARGO_PROFILE_DEV_CODEGEN_BACKEND=cranelift \
# cargo build -Zcodegen-backend $FLAGS --target-dir=target/both
