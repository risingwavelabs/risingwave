#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
# Unset trap on exit set in `common.sh` (diagnose and coverage reporting).
trap - EXIT

echo "--- Download all coverage reports"

# Create a directory for coverage files
mkdir -p coverage-reports

# Download all coverage files from all CI steps
echo "Downloading coverage files..."
buildkite-agent artifact download "coverage-*.lcov" coverage-reports

# List downloaded files for debugging
echo "Downloaded coverage files:"
ls -la coverage-reports/*.lcov || {
    echo "No coverage files found after download"
    exit 1
}

echo "--- Upload to codecov"
codecovcli upload-process --token "$CODECOV_TOKEN" --dir coverage-reports --flag "ci-${BUILDKITE_PIPELINE_SLUG}" --fail-on-error
