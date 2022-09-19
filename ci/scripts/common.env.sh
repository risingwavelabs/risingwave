export CARGO_TERM_COLOR=always
export RUSTFLAGS="-D warnings -Ctarget-cpu=native --cfg tokio_unstable"
export PROTOC_NO_VENDOR=true
export CARGO_HOME=/risingwave/.cargo
export RISINGWAVE_CI=true
