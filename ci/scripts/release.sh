#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

REPO_ROOT=${PWD}
ARCH="$(uname -m)"

echo "--- Check env"
if [ "${BUILDKITE_SOURCE}" != "schedule" ] && [ "${BUILDKITE_SOURCE}" != "webhook" ] && [[ -z "${BINARY_NAME+x}" ]]; then
  exit 0
fi

echo "--- Install aws cli"
curl "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip && ./aws/install && mv /usr/local/bin/aws /bin/aws

echo "--- Install lld"
# The lld in the CentOS 7 repository is too old and contains a bug that causes a linker error.
# So we install a newer version here. (17.0.6, latest version at the time of writing)
# It is manually built in the same environent and uploaded to S3.
aws s3 cp s3://rw-ci-deps-dist/llvm-lld-manylinux2014_${ARCH}.tar.gz .
tar -zxvf llvm-lld-manylinux2014_${ARCH}.tar.gz --directory=/usr/local
ld.lld --version

echo "--- Install dependencies for openssl"
yum install -y perl-core

echo "--- Install java and maven"
yum install -y java-11-openjdk java-11-openjdk-devel wget python3 python3-devel cyrus-sasl-devel
pip3 install toml-cli
wget https://rw-ci-deps-dist.s3.amazonaws.com/apache-maven-3.9.3-bin.tar.gz && tar -zxvf apache-maven-3.9.3-bin.tar.gz
export PATH="${REPO_ROOT}/apache-maven-3.9.3/bin:$PATH"
mvn -v

echo "--- Install rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
source "$HOME/.cargo/env"
rustup show
source ci/scripts/common.sh
unset RUSTC_WRAPPER # disable sccache

echo "--- Install protoc3"
PROTOC_ARCH=${ARCH}
if [ ${ARCH} == "aarch64" ]; then
  # shellcheck disable=SC1068
  PROTOC_ARCH="aarch_64"
fi
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-${PROTOC_ARCH}.zip
unzip -o protoc-3.15.8-linux-${PROTOC_ARCH}.zip -d protoc
mv ./protoc/bin/protoc /usr/local/bin/
mv ./protoc/include/* /usr/local/include/

echo "--- Check risingwave release version"
if [[ -n "${BUILDKITE_TAG}" ]]; then
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
cargo build -p risingwave_cmd --bin risectl --features "rw-static-link" --profile release
cd target/release && chmod +x risingwave risectl

echo "--- Upload nightly binary to s3"
if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
  tar -czvf risingwave-"$(date '+%Y%m%d')"-${ARCH}-unknown-linux.tar.gz risingwave
  aws s3 cp risingwave-"$(date '+%Y%m%d')"-${ARCH}-unknown-linux.tar.gz s3://rw-nightly-pre-built-binary
elif [[ -n "${BINARY_NAME+x}" ]]; then
    tar -czvf risingwave-${BINARY_NAME}-${ARCH}-unknown-linux.tar.gz risingwave
    aws s3 cp risingwave-${BINARY_NAME}-${ARCH}-unknown-linux.tar.gz s3://rw-nightly-pre-built-binary
fi

echo "--- Build connector node"
cd ${REPO_ROOT}/java && mvn -B package -Dmaven.test.skip=true -Dno-build-rust

if [[ -n "${BUILDKITE_TAG}" ]]; then
  echo "--- Collect all release assets"
  cd ${REPO_ROOT} && mkdir release-assets && cd release-assets
  cp -r ${REPO_ROOT}/target/release/* .
  mv ${REPO_ROOT}/java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz risingwave-connector-"${BUILDKITE_TAG}".tar.gz
  tar -zxvf risingwave-connector-"${BUILDKITE_TAG}".tar.gz libs
  ls -l

  echo "--- Install gh cli"
  yum install -y dnf
  dnf install -y 'dnf-command(config-manager)'
  dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
  dnf install -y gh

  echo "--- Release create"
  set +e
  response=$(gh api repos/risingwavelabs/risingwave/releases/tags/${BUILDKITE_TAG} 2>&1)
  set -euo pipefail
  if [[ $response == *"Not Found"* ]]; then
    echo "Tag ${BUILDKITE_TAG} does not exist. Creating release..."
    gh release create "${BUILDKITE_TAG}" --notes "release ${BUILDKITE_TAG}" -d -p
  else
    echo "Tag ${BUILDKITE_TAG} already exists. Skipping release creation."
  fi

  echo "--- Release upload risingwave asset"
  tar -czvf risingwave-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux.tar.gz risingwave
  gh release upload "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux.tar.gz

  echo "--- Release upload risingwave debug info"
  tar -czvf risingwave-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux.dwp.tar.gz risingwave.dwp
  gh release upload "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux.dwp.tar.gz

  echo "--- Release upload risectl asset"
  tar -czvf risectl-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux.tar.gz risectl
  gh release upload "${BUILDKITE_TAG}" risectl-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux.tar.gz

  echo "--- Release upload risingwave-all-in-one asset"
  tar -czvf risingwave-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux-all-in-one.tar.gz risingwave libs
  gh release upload "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-${ARCH}-unknown-linux-all-in-one.tar.gz
fi




