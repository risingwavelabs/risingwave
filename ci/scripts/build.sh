#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

while getopts 't:p:' opt; do
    case ${opt} in
        t )
            target=$OPTARG
            ;;
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

echo "--- Rust cargo-sort check"
cargo sort --check --workspace

echo "--- Rust cargo-hakari check"
cargo hakari generate --diff
cargo hakari verify

echo "--- Rust format check"
cargo fmt --all -- --check

echo "--- Build Rust components"
cargo build \
    -p risingwave_cmd_all \
    -p risedev \
    -p risingwave_regress_test \
    -p risingwave_sqlsmith \
    -p risingwave_compaction_test \
    -p risingwave_backup_cmd \
    -p risingwave_java_binding \
    -p risingwave_e2e_extended_mode_test \
    --features "rw-static-link" \
    --profile "$profile" \
    --timings

# the file name suffix of artifact for risingwave_java_binding is so only for linux. It is dylib for MacOS
artifacts=(risingwave sqlsmith compaction-test backup-restore risingwave_regress_test risingwave_e2e_extended_mode_test risedev-dev delete-range-test librisingwave_java_binding.so)

echo "--- Show link info"
ldd target/"$target"/risingwave

echo "--- Upload artifacts"
echo -n "${artifacts[*]}" | parallel -d ' ' "mv target/$target/{} ./{}-$profile && buildkite-agent artifact upload ./{}-$profile"

buildkite-agent artifact upload target/cargo-timings/cargo-timing.html

echo "--- Show sccache stats"
sccache --show-stats
