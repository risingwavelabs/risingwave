#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Extract DocSlt end-to-end tests"
cargo run --bin risedev-docslt

echo "--- Upload generated end-to-end tests"

echo "--- Debug startup ---"
echo "USER: $(whoami)"
echo "PWD: $(pwd)"
echo "PATH: $PATH"
echo "Filesystem root listing (short):"
ls -ld /* 2>/dev/null || true
echo "--- Checking for buildkite-agent binary in a few likely places ---"

CANDIDATES=(
  "/usr/local/bin/buildkite-agent"
  "/usr/bin/buildkite-agent"
  "/root/.local/bin/buildkite-agent"
  "/data/buildkite/buildkite-agent"
  "/data/buildkite/bin/buildkite-agent"
  "./buildkite-agent"
)

FOUND=""
for p in "${CANDIDATES[@]}"; do
  if [ -x "$p" ]; then
    echo "Found executable buildkite-agent at: $p"
    FOUND="$p"
    break
  elif [ -f "$p" ]; then
    echo "Found file (not executable) at: $p"
    FOUND="$p"
    break
  else
    echo "Not found: $p"
  fi
done

# If found, copy it to cwd and ensure executable and add to PATH
if [ -n "$FOUND" ]; then
  echo "Copying $FOUND -> $PWD/buildkite-agent"
  cp "$FOUND" ./buildkite-agent
  chmod +x ./buildkite-agent
  export PATH="$PWD:$PATH"
  echo "Updated PATH: $PATH"
else
  echo "No existing buildkite-agent binary found in candidates."
  echo "Attempting to download a standalone buildkite-agent binary (fallback)."
  # Download fallback: try official Buildkite release tarball (x86_64 linux)
  # NOTE: network may be blocked in some environments; this is best-effort.
  set +e
  curl -fsSL "https://github.com/buildkite/agent/releases/latest/download/buildkite-agent-linux-amd64" -o ./buildkite-agent || true
  if [ -f ./buildkite-agent ]; then
    chmod +x ./buildkite-agent
    export PATH="$PWD:$PATH"
    echo "Downloaded buildkite-agent and added to PATH."
  else
    echo "Failed to download buildkite-agent. You will need to ensure the binary is present in the container or mounted into it."
    echo "Candidates checked: ${CANDIDATES[*]}"
    # Do not exit here; let script fail naturally when trying to run buildkite-agent later (set -e still applies)
  fi
  set -e
fi

echo "Command lookup: $(command -v buildkite-agent || true)"
echo "buildkite-agent --version (if available):"
buildkite-agent --version || true


tar --zstd -cvf e2e_test_generated.tar.zst e2e_test/generated
buildkite-agent artifact upload e2e_test_generated.tar.zst
