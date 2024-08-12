#!/usr/bin/env bash

# Usage: ./run_command.sh "log path" "status path" command

echo "${@:3}"
echo "logging to $1, and status to $2"

# Strip ANSI color codes to make the log file readable.
# - trap '' INT: ignore interrupts in sed
# -      sed -u: unbuffered output
strip_ansi() {
  (trap '' INT; sed -u -e 's/\x1b\[[0-9;]*m//g')
}

# Run the command and log the output to both the terminal and the log file.
# - CLICOLOR_FORCE=1: force color output, see https://bixense.com/clicolors/
# -           tee -i: ignore interrupts in tee
CLICOLOR_FORCE=1 "${@:3}" 2>&1 | tee -i >(strip_ansi > "$1")

# Retrieve the return status.
RET_STATUS="${PIPESTATUS[0]}"

# If the status file exists, write the return status to it.
#
# The status file is used to detect early exits. Once `risedev-dev` finishes launching successfully,
# it will clean up the status file, so it's safe to ignore it then.
if [ -f "$2" ]; then
    echo "status ${RET_STATUS}" > "$2"
fi

# Show the return status in both outputs.
echo "$(date -u) [risedev]: Program exited with ${RET_STATUS}" >> "$1"
echo "Program exited with ${RET_STATUS}, press Ctrl+C again to close the window."

while true; do
    read -r
done
