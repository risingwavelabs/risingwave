SHELL := /bin/bash
.PHONY: all java java_test java_build java_check
all: java rust

java: java_test

java_test:
	cd java && ./gradlew test

java_build:
	cd java && ./gradlew build

java_check:
	cd java && ./gradlew check

java_coverage_report:
	cd java && ./gradlew jacocoRootReport

rust: rust_check_all rust_test

rust_check_all: rust_fmt_check rust_clippy_check rust_cargo_sort_check

rust_fmt_check:
	cd rust && cargo fmt --all -- --check

rust_clippy_check_locked:
	cd rust && cargo clippy --all-targets --locked -- -D warnings

rust_clippy_check:
	cd rust && cargo clippy --all-targets -- -D warnings

rust_cargo_sort_check:
	cd rust && cargo sort -c -w

rust_test:
	cd rust && RUSTFLAGS=-Dwarnings cargo test

rust_test_no_run:
	cd rust && RUSTFLAGS=-Dwarnings cargo test --no-run

# Note: "--skip-clean" must be used along with "CARGO_TARGET_DIR=..."
# See also https://github.com/xd009642/tarpaulin/issues/777
rust_test_with_coverage:
	cd rust && RUSTFLAGS=-Dwarnings CARGO_TARGET_DIR=target/tarpaulin cargo tarpaulin --workspace \
	  --exclude risingwave_pb --exclude-files tests/* --out Xml --force-clean --run-types Doctests Tests \
	  -- --report-time -Z unstable-options

rust_build:
	cd rust && cargo build

rust_clean:
	cd rust && cargo clean

rust_clean_build:
	cd rust && cargo clean && cargo build

rust_doc:
	cd rust && cargo doc --workspace --no-deps --document-private-items

# state store bench
ss_bench_build:
	cd rust && cargo build --workspace --bin ss-bench

sqllogictest:
	cargo install --git https://github.com/risinglightdb/sqllogictest-rs --features bin

export DOCKER_GROUP_NAME ?= risingwave
export DOCKER_IMAGE_TAG ?= latest
export DOCKER_COMPONENT_BACKEND_NAME ?= backend
export DOCKER_COMPONENT_FRONTEND_NAME ?= frontend

docker: docker_frontend docker_backend

docker_frontend:
	docker build -f docker/frontend/Dockerfile -t ${DOCKER_GROUP_NAME}/${DOCKER_COMPONENT_FRONTEND_NAME}:${DOCKER_IMAGE_TAG} .

docker_backend:
	docker build -f docker/backend/Dockerfile -t ${DOCKER_GROUP_NAME}/${DOCKER_COMPONENT_BACKEND_NAME}:${DOCKER_IMAGE_TAG} .
