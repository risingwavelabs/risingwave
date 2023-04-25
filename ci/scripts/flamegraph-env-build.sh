#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

############# INSTALL NEXMARK BENCH

echo "CUR_DIR: $PWD"
pushd ..
git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/nexmark-bench.git
pushd nexmark-bench
# TODO(kwannoel): Upstream this fix
echo "nightly-2023-04-07" > rust-toolchain
make install
cp /risingwave/.cargo/bin/nexmark-server ./nexmark-server
buildkite-agent artifact upload ./nexmark-server
popd
popd

############# SETUP RW

# TODO(kwannoel): Currently these are in `gen-flamegraph.sh`.
#
# That's sub-optimal as it takes up disk space, which could otherwise
# be used to store more nexmark events.
# This happens because we rely on `risedev` to start the cluster.
#
# To fix we need to decouple build and run steps for `risedev`,
# so they can run in separate buildkite steps.
#
# Specifically: Setup downloaded artifacts built by `risedev`
# such that they can be used directly by `risedev` when starting the cluster.
# Currently even when these artifacts are present in `target/release`,
# `risedev` still rebuilds them.