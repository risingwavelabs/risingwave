export CARGO_TERM_COLOR=always
export PROTOC_NO_VENDOR=true
export CARGO_HOME=/risingwave/.cargo
export RISINGWAVE_CI=true
export RUST_BACKTRACE=1
export ENABLE_TELEMETRY=false

if [ -n "${BUILDKITE_COMMIT:-}" ]; then
  export GIT_SHA=$BUILDKITE_COMMIT
fi
