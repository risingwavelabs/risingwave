export CARGO_TERM_COLOR=always
export PROTOC_NO_VENDOR=true
export CARGO_HOME=/risingwave/.cargo
export RISINGWAVE_CI=true
export RUST_BACKTRACE=1
export ENABLE_TELEMETRY=false
export RUSTC_WRAPPER=sccache
export RUSTC_WORKSPACE_WRAPPER=/risingwave/ci/scripts/rustc-workspace-wrapper.sh
export SCCACHE_BUCKET=rw-ci-sccache-bucket
export SCCACHE_REGION=us-east-2
export SCCACHE_IDLE_TIMEOUT=0
export CARGO_INCREMENTAL=0
export CARGO_MAKE_PRINT_TIME_SUMMARY=true
export MINIO_DOWNLOAD_BIN=https://rw-ci-deps-dist.s3.amazonaws.com/minio
export MCLI_DOWNLOAD_BIN=https://rw-ci-deps-dist.s3.amazonaws.com/mc
export GCLOUD_DOWNLOAD_TGZ=https://rw-ci-deps-dist.s3.amazonaws.com/google-cloud-cli-475.0.0-linux-x86_64.tar.gz
export NEXTEST_HIDE_PROGRESS_BAR=true
export RW_TELEMETRY_TYPE=test
export RW_SECRET_STORE_PRIVATE_KEY_HEX="0123456789abcdef0123456789abcdef"
export SLT_FAIL_FAST=true
export SLT_KEEP_DB_ON_FAILURE=true
export SLT_SHUTDOWN_TIMEOUT=10
export RW_BUILD_INSTRUMENT_COVERAGE=1
export LLVM_PROFILE_FILE='/risingwave/target/risingwave-%p.profraw'
export CARGO_LLVM_COV=1
export CARGO_LLVM_COV_SHOW_ENV=1
export CARGO_LLVM_COV_TARGET_DIR=/risingwave/target

unset LANG

function generate_and_upload_coverage_report() {
  echo "--- Generate coverage report"

  # Generate unique coverage filename with job id.
  coverage_filename="coverage-${BUILDKITE_JOB_ID}.lcov"

  # Only generate coverage if we have profraw files.
  if [ -n "$(find target -name '*.profraw' 2>/dev/null | head -1)" ]; then
    len=$(find target -name '*.profraw' 2>/dev/null | wc -l)
    echo "Found ${len} profraw files. Generating coverage report."

    cargo llvm-cov report --lcov --output-path "$coverage_filename" || {
      echo "Warning: Failed to generate coverage report"
    }

    if [ -f "$coverage_filename" ]; then
      buildkite-agent artifact upload "$coverage_filename" || {
        echo "Warning: Failed to upload coverage report"
      }
    fi
  else
    echo "No profraw files found. Skipping coverage generation."
  fi
}


function exit_hook() {
  ret=$?

  # Generate and upload coverage report on successful completion
  if [ $ret -eq 0 ]; then
    generate_and_upload_coverage_report
    exit 0
  fi

  # Otherwise, print diagnose info
  echo "^^^ +++"
  echo "--- Failed to run command! Dumping diagnose info..."
  if [ -f .risingwave/config/risedev-env ]; then
    ./risedev diagnose || true
  fi
}
trap exit_hook EXIT

if [ -n "${BUILDKITE_COMMIT:-}" ]; then
  export GIT_SHA=$BUILDKITE_COMMIT
fi

# Arguments:
#   $1: filename (It should be in the current directory)
function compress-and-upload-artifact() {
  tar --zstd -cvf "$1".tar.zst "$1"
  buildkite-agent artifact upload "$1".tar.zst
}

# Arguments:
#   $1: artifact name
#   $2: output directory
function download-and-decompress-artifact() {
  buildkite-agent artifact download "$1".tar.zst "$2"
  tar -xvf "$2"/"$1".tar.zst -C "$2" --no-same-owner
}

# export functions so they can be used in parallel
export -f compress-and-upload-artifact
export -f download-and-decompress-artifact

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
  download-and-decompress-artifact risingwave-"$profile" target/debug/
  download-and-decompress-artifact risedev-dev-"$profile" target/debug/

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

  risedev pre-start-dev
  risedev link-all-in-one-binaries
}

function filter_stack_trace() {
  # Only keep first 3 lines of backtrace: 0-2.
  echo "filtering stack trace for $1"
  touch tmp
  cat "$1" \
  | sed -E '/  [1-9][0-9]+:/d' \
  | sed -E '/  at .rustc/d' \
  | sed -E '/  at ...cargo/d' > tmp
  cp tmp "$1"
  rm tmp
}

get_latest_kafka_version() {
    local versions=$(curl -s https://downloads.apache.org/kafka/ | grep -Eo 'href="[0-9]+\.[0-9]+\.[0-9]+/"' | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+")
    # Sort the version numbers and get the latest one
    local latest_version=$(echo "$versions" | sort -V | tail -n1)
    echo "$latest_version"
}

get_latest_kafka_download_url() {
    local latest_version=$(get_latest_kafka_version)
    local download_url="https://downloads.apache.org/kafka/${latest_version}/kafka_2.13-${latest_version}.tgz"
    echo "$download_url"
}

get_latest_cassandra_version() {
    local versions=$(curl -s https://downloads.apache.org/cassandra/ | grep -Eo 'href="[0-9]+\.[0-9]+\.[0-9]+/"' | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+")
    # Sort the version numbers and get the latest one
    local latest_version=$(echo "$versions" | sort -V | tail -n1)
    echo "$latest_version"
}

get_latest_cassandra_download_url() {
    local latest_version=$(get_latest_cassandra_version)
    local download_url="https://downloads.apache.org/cassandra/${latest_version}/apache-cassandra-${latest_version}-bin.tar.gz"
    echo "$download_url"
}

configure_static_openssl() {
    export OPENSSL_STATIC=1
    export OPENSSL_LIB_DIR="$(dpkg -L libssl-dev | grep libssl.a | xargs dirname)"
    export OPENSSL_INCLUDE_DIR="$(dpkg -L libssl-dev | grep openssl/ssl.h | xargs dirname)"
    echo "OPENSSL_STATIC: $OPENSSL_STATIC"
    echo "OPENSSL_LIB_DIR: $OPENSSL_LIB_DIR"
    echo "OPENSSL_INCLUDE_DIR: $OPENSSL_INCLUDE_DIR"
}

check_link_info() {
  profile=$1
  ldd_output=$(ldd target/"$profile"/risingwave)
  echo "$ldd_output"
  # enforce that libssl is not present if we are building with static openssl
  if [[ "$profile" == "ci-release" || "$profile" == "production" ]] && [[ "$ldd_output" == *"libssl"* ]]; then
      echo "libssl should not be dynamically linked"
      exit 1
  fi
}

# Set nextest partition argument based on BuildKite parallel job configuration
if [ -n "${BUILDKITE_PARALLEL_JOB:-}" ] && [ -n "${BUILDKITE_PARALLEL_JOB_COUNT:-}" ]; then
  # Add 1 to BUILDKITE_PARALLEL_JOB to get 1-based index
  NEXTEST_PARTITION_ARG="--partition hash:$((BUILDKITE_PARALLEL_JOB + 1))/${BUILDKITE_PARALLEL_JOB_COUNT}"
  echo "NEXTEST_PARTITION_ARG is set to \"$NEXTEST_PARTITION_ARG\" based on Buildkite parallel job configuration."
else
  NEXTEST_PARTITION_ARG=""
fi
