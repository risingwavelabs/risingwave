#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

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

echo "--- Install aws cli"
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip && ./aws/install && mv /usr/local/bin/aws /bin/aws

echo "--- Test s3 download speed"
aws s3 cp --acl private --sse aws:kms s3://bulidkite-artifacts-bucket/9eed51d0-eaaf-4ea3-95a2-2e9e557607f6/01864a93-fd30-4d57-978a-9ea2a2bdd09e/01864a94-2598-4e55-bb19-b8ab16a0a109/risingwave_simulation .

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
    --features "static-link static-log-level" --profile "$profile"

artifacts=(risingwave sqlsmith compaction-test backup-restore risingwave_regress_test risedev-dev delete-range-test)

echo "--- Compress debug info for artifacts"
echo -n "${artifacts[*]}" | parallel -d ' ' "objcopy --compress-debug-sections=zlib-gnu target/$target/{} && echo \"compressed {}\""

echo "--- Show link info"
ldd target/"$target"/risingwave

echo "--- Upload artifacts"
echo -n "${artifacts[*]}" | parallel -d ' ' "mv target/$target/{} ./{}-$profile && buildkite-agent artifact upload ./{}-$profile"

echo "--- Show sccache stats"
sccache --show-stats
