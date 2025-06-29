#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
# Unset trap on exit set in `common.sh` (diagnose and coverage reporting).
trap - EXIT

echo "--- Download all coverage reports"

# Create a directory for coverage files
mkdir -p coverage-reports
cd coverage-reports

# Download all coverage files from all CI steps
echo "Downloading coverage files..."
buildkite-agent artifact download "coverage-*.lcov" .

# List downloaded files for debugging
echo "Downloaded coverage files:"
ls -la ./*.lcov || {
    echo "No coverage files found after download"
    exit 1
}

echo "--- Upload to codecov"

# TODO: preinstall CLI in CI image
echo "Installing codecov CLI..."
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov

echo "Uploading coverage reports to codecov..."
./codecov -t "$CODECOV_TOKEN" -s . -F "ci-${BUILDKITE_PIPELINE_SLUG}"
