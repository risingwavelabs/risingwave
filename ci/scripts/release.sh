#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}

echo "--- Check env"
if [ "${BUILDKITE_SOURCE}" != "schedule" ] && [ "${BUILDKITE_SOURCE}" != "webhook" ] && [[ -z "${BINARY_NAME+x}" ]]; then
  exit 0
fi

echo "--- Install java and maven"
yum install -y java-11-openjdk wget python3
pip3 install toml-cli
wget https://dlcdn.apache.org/maven/maven-3/3.9.2/binaries/apache-maven-3.9.2-bin.tar.gz && tar -zxvf apache-maven-3.9.2-bin.tar.gz
export PATH="${REPO_ROOT}/apache-maven-3.9.2/bin:$PATH"
mvn -v

echo "--- Install rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
source "$HOME/.cargo/env"
rustup show
source ci/scripts/common.sh
unset RUSTC_WRAPPER # disable sccache

echo "--- Install protoc3"
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
unzip -o protoc-3.15.8-linux-x86_64.zip -d protoc
mv ./protoc/bin/protoc /usr/local/bin/
mv ./protoc/include/* /usr/local/include/

echo "--- Install lld"
yum install -y centos-release-scl-rh
yum install -y llvm-toolset-7.0-lld
source /opt/rh/llvm-toolset-7.0/enable

echo "--- Install aws cli"
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip && ./aws/install && mv /usr/local/bin/aws /bin/aws

echo "--- Check risingwave release version"
if [[ -n "${BUILDKITE_TAG+x}" ]]; then
  CARGO_PKG_VERSION="$(toml get --toml-path Cargo.toml workspace.package.version)"
  if [[ "${CARGO_PKG_VERSION}" != "${BUILDKITE_TAG#*v}" ]]; then
    echo "CARGO_PKG_VERSION: ${CARGO_PKG_VERSION}"
    echo "BUILDKITE_TAG: ${BUILDKITE_TAG}"
    echo "CARGO_PKG_VERSION and BUILDKITE_TAG are not equal"
    exit 1
  fi
fi

echo "--- Build risingwave release binary"
cargo build -p risingwave_cmd_all --features "rw-static-link" --profile release
cargo build --bin risectl --features "rw-static-link" --profile release
cd target/release && chmod +x risingwave risectl

echo "--- Upload nightly binary to s3"
if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
  tar -czvf risingwave-"$(date '+%Y%m%d')"-x86_64-unknown-linux.tar.gz risingwave
  aws s3 cp risingwave-"$(date '+%Y%m%d')"-x86_64-unknown-linux.tar.gz s3://risingwave-nightly-pre-built-binary
elif [[ -n "${BINARY_NAME+x}" ]]; then
    tar -czvf risingwave-${BINARY_NAME}-x86_64-unknown-linux.tar.gz risingwave
    aws s3 cp risingwave-${BINARY_NAME}-x86_64-unknown-linux.tar.gz s3://risingwave-nightly-pre-built-binary
fi

if [[ -n "${BUILDKITE_TAG+x}" ]]; then
  echo "--- Install gh cli"
  yum install -y dnf
  dnf install -y 'dnf-command(config-manager)'
  dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
  dnf install -y gh

  echo "--- Release create"
  gh release create "${BUILDKITE_TAG}" --notes "release ${BUILDKITE_TAG}" -d -p

  echo "--- Release upload risingwave asset"
  tar -czvf risingwave-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz risingwave
  gh release upload "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz

  echo "--- Release upload risectl asset"
  tar -czvf risectl-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz risectl
  gh release upload "${BUILDKITE_TAG}" risectl-"${BUILDKITE_TAG}"-x86_64-unknown-linux.tar.gz

  echo "--- Release build and upload risingwave connector node jar asset"
  cd ${REPO_ROOT}/java && mvn -B package -Dmaven.test.skip=true
  cd connector-node/assembly/target && mv risingwave-connector-1.0.0.tar.gz risingwave-connector-"${BUILDKITE_TAG}".tar.gz
  gh release upload "${BUILDKITE_TAG}" risingwave-connector-"${BUILDKITE_TAG}".tar.gz
fi




