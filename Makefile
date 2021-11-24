SHELL := /bin/bash
.PHONY: all java java_test java_build java_check
all: cpp java

java: java_test

java_test:
	cd java && ./gradlew test

java_build:
	cd java && ./gradlew build

java_check:
	cd java && ./gradlew check

java_coverage_report:
	cd java && ./gradlew jacocoRootReport

sqllogictest:
	cd go/sqllogictest && make
	mkdir -p go/bin
	cp go/sqllogictest/bin/sqllogictest go/bin/sqllogictest

rust: rust_fmt rust_check rust_test

rust_fmt:
	cd rust && cargo fmt --all -- --check

rust_check:
	cd rust && cargo clippy --all-targets --all-features -- -D warnings

rust_cargo_sort_check:
	cd rust && cargo sort -c -w

rust_test:
	cd rust && mkdir -p proto
	cd rust && RUSTFLAGS=-Dwarnings cargo test

# Note: "--skip-clean" must be used along with "CARGO_TARGET_DIR=..."
# See also https://github.com/xd009642/tarpaulin/issues/777
rust_test_with_coverage:
	cd rust && mkdir -p proto
	cd rust && RUSTFLAGS=-Dwarnings CARGO_TARGET_DIR=target_tarpaulin cargo tarpaulin --workspace --exclude risingwave-proto --exclude-files tests/* --out Xml --skip-clean

rust_build:
	cd rust && cargo build

rust_clean:
	cd rust && cargo clean

rust_clean_build:
	cd rust && cargo clean && cargo build

rust_doc:
	cd rust && cargo doc --workspace --no-deps --document-private-items
