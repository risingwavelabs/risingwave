#!/usr/bin/env bash

set -euo pipefail

print_machine_debug_info() {
  echo "Free space on Machine"
  df -h
  echo "Files on Machine"
  ls
}

echo "--- Machine Debug Info"
print_machine_debug_info
echo "--- Setting up RW"
cd risingwave
ENABLE_RW_RELEASE_PROFILE=true ./risedev d bench
echo "Success!"
echo "--- Running Benchmarks"
echo "Success!"
echo "--- Generating flamegraph"
echo "Success!"