export CARGO_TERM_COLOR=always
export PROTOC_NO_VENDOR=true
export CARGO_HOME=/risingwave/.cargo
export RISINGWAVE_CI=true
export RUST_BACKTRACE=1
export ENABLE_TELEMETRY=false
export MINIO_DOWNLOAD_BIN=https://ci-deps-dist.s3.amazonaws.com/minio
export MCLI_DOWNLOAD_BIN=https://ci-deps-dist.s3.amazonaws.com/mc
export GCLOUD_DOWNLOAD_TGZ=https://ci-deps-dist.s3.amazonaws.com/google-cloud-cli-406.0.0-linux-x86_64.tar.gz

if [ -n "${BUILDKITE_COMMIT:-}" ]; then
  export GIT_SHA=$BUILDKITE_COMMIT
fi

# Arguments:
#   $1: cargo build `profile` of the binaries
#   $2: risedev-components `env` to use
# 
# Download risingwave and risedev-dev, and put them in target/debug
function download_and_prepare_rw() {
  echo "--- Download RisingWave binaries and prepare environment"
  if [ -z "$1" ]; then
    echo "download_and_prepare_rw: missing argument profile"
    exit 1
  fi
  if [ -z "$2" ]; then
    echo "download_and_prepare_rw: missing argument env"
    exit 1
  fi
  # env is either common or source
  if [ "$2" != "common" ] && [ "$2" != "source" ]; then
    echo "download_and_prepare_rw: invalid argument env"
    exit 1
  fi

  profile=$1
  env=$2

  echo -e "\033[33mDownload artifacts\033[0m"

  mkdir -p target/debug
  buildkite-agent artifact download risingwave-"$profile" target/debug/
  buildkite-agent artifact download risedev-dev-"$profile" target/debug/

  mv target/debug/risingwave-"$profile" target/debug/risingwave
  mv target/debug/risedev-dev-"$profile" target/debug/risedev-dev

  chmod +x ./target/debug/risingwave
  chmod +x ./target/debug/risedev-dev

  echo -e "\033[33mGenerate RiseDev CI config\033[0m"

  if [ "$env" = "common" ]; then
    cp ci/risedev-components.ci.env risedev-components.user.env
  elif [ "$env" = "source" ]; then
    cp ci/risedev-components.ci.source.env risedev-components.user.env
  fi

  echo -e "\033[33mPrepare RiseDev dev cluster\033[0m"

  cargo make pre-start-dev
  cargo make link-all-in-one-binaries
}

# Arguments:
#   $1: cargo build `profile` of the binaries
function download_java_binding() {
  echo "--- Download java binding"
  if [ -z "$1" ]; then
    echo "download_java_binding: missing argument profile"
    exit 1
  fi

  profile=$1

  echo -e "\033[33mDownload artifacts\033[0m"

  mkdir -p target/debug
  buildkite-agent artifact download librisingwave_java_binding.so-"$profile" target/debug
  mv target/debug/librisingwave_java_binding.so-"$profile" target/debug/librisingwave_java_binding.so
  
  export RW_JAVA_BINDING_LIB_PATH=${PWD}/target/debug
}
