#!/usr/bin/env bash
set -euo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

tests=( \
"test_basic.sh" \
"test_pin_sst.sh" \
"test_query_backup.sh" \
"test_set_config.sh" \
)
for t in "${tests[@]}"
do
  bash "${DIR}/${t}"
done

echo "all tests succeeded"