#!/bin/bash

# Exits as soon as any line fails.
# set -e

# this needs to be run from risingwave root dir
cat << EOF > .cargo/config.toml
# Add "-Ctarget-feature=+avx2" if your x86_64 target supports AVX2 vector extensions
[target.x86_64-unknown-linux-gnu]
rustflags = [
    "-Clink-arg=-fuse-ld=lld", "-Clink-arg=-Wl,--no-rosegment", "--cfg", "tokio_unstable"
]

# Add "-Ctarget-feature=+neon" if your aarch64 target supports NEON vector extensions
[target.aarch64-unknown-linux-gnu]
rustflags = [
    "-Clink-arg=-fuse-ld=lld", "-Clink-arg=-Wl,--no-rosegment", "--cfg", "tokio_unstable"
]

[build]
rustflags = ["-Ctarget-cpu=native", "--cfg", "tokio_unstable"]
EOF