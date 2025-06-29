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

# Enable coverage instrumentation.
export RW_BUILD_INSTRUMENT_COVERAGE=1

echo "--- Build Rust components"

if [[ "$profile" == "ci-dev" ]]; then
    RISINGWAVE_FEATURE_FLAGS=(--features rw-dynamic-link --no-default-features)
else
    RISINGWAVE_FEATURE_FLAGS=(--features rw-static-link)
    configure_static_openssl
fi

cargo build \
    -p risingwave_cmd_all \
    -p risedev \
    -p risingwave_regress_test \
    -p risingwave_sqlsmith \
    -p risingwave_compaction_test \
    -p risingwave_e2e_extended_mode_test \
    "${RISINGWAVE_FEATURE_FLAGS[@]}" \
    --features udf \
    --profile "$profile" \
    --timings


artifacts=(risingwave sqlsmith compaction-test risingwave_regress_test risingwave_e2e_extended_mode_test risedev-dev)

echo "--- Check link info"
check_link_info "$profile"

echo "--- Upload artifacts"
echo -n "${artifacts[*]}" | parallel -d ' ' "mv target/$profile/{} ./{}-$profile && compress-and-upload-artifact ./{}-$profile"
buildkite-agent artifact upload target/cargo-timings/cargo-timing.html

# This magically makes it faster to exit the docker
rm -rf target

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats
