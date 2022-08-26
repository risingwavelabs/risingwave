#!/bin/bash

# Usage: ./run_command.sh "log path" "status path" command

echo "${@:3}"
echo "logging to $1, and status to $2"

"${@:3}" 2>&1 | tee -a "$1"

RET_STATUS="${PIPESTATUS[0]}"

echo "status ${RET_STATUS}" > "$2"

echo "$(date -u) [risedev]: Program exited with ${RET_STATUS}" >> "$1"
echo "Program exited with ${RET_STATUS}, press Ctrl+C to continue."

while true; do
    read -r
done
