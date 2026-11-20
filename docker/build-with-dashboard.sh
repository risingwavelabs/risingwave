#!/usr/bin/env bash

set -euo pipefail

sync_dir=$(mktemp -d)
dashboard_pid=
cleanup() {
  if [[ -n "$dashboard_pid" ]]; then
    kill -- "-$dashboard_pid" 2>/dev/null || true
    wait "$dashboard_pid" || true
  fi
  rm -rf -- "$sync_dir"
}
trap cleanup EXIT

# Give the Dashboard job a process group so cleanup also stops pnpm's children.
set -m
exec 9>"$sync_dir/lock"
flock --exclusive 9
(
  cd dashboard
  pnpm install --frozen-lockfile
  pnpm run build
  touch "$sync_dir/ready"
) &
dashboard_pid=$!
exec 9>&-

export RW_DASHBOARD_BUILD_SYNC_DIR="$sync_dir"
cargo fetch
cargo build "$@"
wait "$dashboard_pid"
dashboard_pid=
