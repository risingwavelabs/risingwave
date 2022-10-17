#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- Install rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain $(cat ./rust-toolchain) -y
source "$HOME/.cargo/env"
source ci/scripts/common.env.sh

echo "--- Install protoc3"
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
unzip -o protoc-3.15.8-linux-x86_64.zip -d /usr/local bin/protoc

echo "--- Install gh cli"
yum install -y dnf
dnf install -y 'dnf-command(config-manager)'
dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
dnf install -y gh

echo "--- Install ldd"
yum install -y centos-release-scl-rh
yum install -y llvm-toolset-7.0-lld
source /opt/rh/llvm-toolset-7.0/enable

echo "--- Release create"
gh release create "${BUILDKITE_TAG}" --generate-notes -d -p

echo "--- Build release asset"
cargo build -p risingwave_cmd_all --features "static-link static-log-level" --profile release
cd target/release
chmod +x risingwave
tar -czvf risingwave-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz risingwave

echo "--- Release upload asset"
gh release upload "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz
