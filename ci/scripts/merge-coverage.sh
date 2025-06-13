#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Download all coverage reports"

# Create a directory for coverage files
mkdir -p coverage-reports
cd coverage-reports

# Download all coverage files from all CI steps
echo "Downloading coverage files..."
buildkite-agent artifact download "coverage-*.lcov"

# List downloaded files for debugging
echo "Downloaded coverage files:"
ls -la ./*.lcov || {
    echo "No coverage files found after download"
    exit 1
}

# echo "--- Merge coverage reports"

# # Install lcov if not available
# # TODO: preinstall in CI image
# if ! command -v lcov &> /dev/null; then
#     echo "Installing lcov..."
#     apt-get update && apt-get install -y lcov
# fi

# # Merge all lcov files
# lcov_files=(*.lcov)
# echo "Merging ${#lcov_files[@]} coverage files..."
# lcov_args=()
# for file in *.lcov; do
#     lcov_args+=("-a" "$file")
# done
# lcov "${lcov_args[@]}" -o merged-coverage.lcov
# echo "Successfully merged ${#lcov_files[@]} coverage files"

echo "--- Upload to codecov"

echo "Installing codecov CLI..."
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov

# Upload merged coverage to codecov
echo "Uploading merged coverage to codecov..."
./codecov -t "$CODECOV_TOKEN" -s . -F ci-merged
