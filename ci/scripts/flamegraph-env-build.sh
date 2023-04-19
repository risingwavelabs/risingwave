#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Installing go toolchain"
wget https://go.dev/dl/go1.20.3.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.20.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

echo "--- Installing promql cli client"
git clone https://github.com/nalbury/promql-cli.git
pushd promql-cli/
OS=linux INSTALL_PATH=../target/release make install
popd

# FIXME(kwannoel): Not sure if risingwave_java_binding is needed
echo "--- Build Rust components"
cargo build \
    -p risingwave_cmd_all \
    -p risedev \
    -p risingwave_java_binding \
    --features "static-link static-log-level" --profile release

# the file name suffix of artifact for risingwave_java_binding is so only for linux. It is dylib for MacOS
artifacts=(promql risingwave risedev-dev librisingwave_java_binding.so)

echo "--- Show link info"
ldd target/release/risingwave

# Namespacing is required (by suffixing bench: XXX-bench), so we can upload and download buildkite artifacts.
echo "--- Upload artifacts"
echo -n "${artifacts[*]}" | parallel -d ' ' "mv target/release/{} ./{}-bench && buildkite-agent artifact upload ./{}-bench"

echo "--- Show sccache stats"
sccache --show-stats
