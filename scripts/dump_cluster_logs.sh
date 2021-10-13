#!/bin/bash

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/.." || exit 1

for out_file in ./log/*.out
do
  echo ""
  echo "==="
  echo "=== Dump log file $out_file ==="
  echo "==="
  echo ""
  cat "$out_file"
done
