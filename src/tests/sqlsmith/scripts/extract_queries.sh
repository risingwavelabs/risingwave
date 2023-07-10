#!/usr/bin/env bash

# 1. Extract queries from the SQLsmith log file.
# 2. Reduce queries to a minimal set of statements that still trigger the same error.

# NOTE: This script is used locally for debugging. It is not used by the CI.
# USAGE: ./extract_queries.sh <log_file> <output_file>
# It should output $OUTPUT_FILE and $SHRUNK_OUTPUT_FILE.

set -euo pipefail

LOG_FILE="$1"
OUTPUT_FILE="$2"
SHRUNK_OUTPUT_FILE="$2".shrunk

echo "--- Extracting queries"
cat "$LOG_FILE" | rg "\[EXECUTING .*\]" | sed 's/.*\[EXECUTING .*\]: //' | sed 's/$/;/' > "$OUTPUT_FILE"
echo "--- Extracted queries to $LOG_FILE"

echo "--- Shrinking queries"
cargo run --bin sqlsmith-reducer -- --input-file "$OUTPUT_FILE" --output-file "$SHRUNK_OUTPUT_FILE"
echo "--- Shrunk queries to $SHRUNK_OUTPUT_FILE"
