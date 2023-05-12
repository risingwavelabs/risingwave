#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
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

# profile is either ci-dev or ci-release
if [[ "$profile" != "ci-dev" ]] && [[ "$profile" != "ci-release" ]]; then
    echo "Invalid option: profile must be either ci-dev or ci-release" 1>&2
    exit 1
fi

echo "--- Rust cargo-sort check"
cargo sort --check --workspace

echo "--- Rust cargo-hakari check"
cargo hakari generate --diff
cargo hakari verify

echo "--- Rust format check"
cargo fmt --all -- --check

echo "--- Build Rust components"

if [[ "$profile" == "ci-dev" ]]; then
    RISINGWAVE_FEATURE_FLAGS="--features rw-dynamic-link --no-default-features"
else 
    RISINGWAVE_FEATURE_FLAGS="--features rw-static-link"
fi

cargo build \
    -p risingwave_cmd_all \
    -p risedev \
    -p risingwave_regress_test \
    -p risingwave_sqlsmith \
    -p risingwave_compaction_test \
    -p risingwave_backup_cmd \
    -p risingwave_java_binding \
    -p risingwave_e2e_extended_mode_test \
    $RISINGWAVE_FEATURE_FLAGS \
    --profile "$profile"

# the file name suffix of artifact for risingwave_java_binding is so only for linux. It is dylib for MacOS
artifacts=(risingwave sqlsmith compaction-test backup-restore risingwave_regress_test risingwave_e2e_extended_mode_test risedev-dev delete-range-test librisingwave_java_binding.so)

echo "--- Show link info"
ldd target/"$profile"/risingwave

echo "--- Upload artifacts"
echo -n "${artifacts[*]}" | parallel -d ' ' "mv target/$profile/{} ./{}-$profile && buildkite-agent artifact upload ./{}-$profile"

echo "--- Show sccache stats"
sccache --show-stats
