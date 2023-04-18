#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# FIXME(kwannoel): Not sure if risingwave_java_binding is needed
echo "--- Build Rust components"
cargo build \
    -p risingwave_cmd_all \
    -p risedev \
    -p risingwave_java_binding \
    --features "static-link static-log-level" --profile release

# the file name suffix of artifact for risingwave_java_binding is so only for linux. It is dylib for MacOS
artifacts=(risingwave sqlsmith compaction-test backup-restore risingwave_regress_test risingwave_e2e_extended_mode_test risedev-dev delete-range-test librisingwave_java_binding.so)

echo "--- Show link info"
ldd target/release/risingwave

echo "--- Upload artifacts"
echo -n "${artifacts[*]}" | parallel -d ' ' "mv target/release/{} ./{}-release && buildkite-agent artifact upload ./{}-release"

echo "--- Show sccache stats"
sccache --show-stats
