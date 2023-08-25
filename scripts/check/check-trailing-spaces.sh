#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Shell colors
RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
BOLD='\033[1m'
NONE='\033[0m'

_echo_err() {
    echo -e "${RED}$@${NONE}"
}

fix=false
while [ $# -gt 0 ]; do
    case $1 in
    -f | --fix)
        fix=true
        ;;
    *)
        _echo_err "$self: invalid option \`$1\`\n"
        exit 1
        ;;
    esac
    shift
done

# The following is modified from https://github.com/raisedevs/find-trailing-whitespace/blob/restrict-to-plaintext-only/entrypoint.sh.

has_trailing_spaces=false

for file in $(git grep --cached -Il '' -- ':!src/tests/regress/data'); do
    lines=$(grep -E -rnIH "[[:space:]]+$" "$file" | cut -f-2 -d ":" || echo "")
    if [ ! -z "$lines" ]; then
        if [[ $has_trailing_spaces == false ]]; then
            echo -e "\nLines containing trailing whitespace:\n"
            has_trailing_spaces=true
        fi
        if [[ $fix == true ]]; then
            sed -i '' -e's/[[:space:]]*$//' "$file"
        fi
        echo -e "${BLUE}$lines${NONE}"
    fi
done

if [[ $has_trailing_spaces == true ]]; then
    if [[ $fix == false ]]; then
        echo
        echo -e "${RED}${BOLD}Please clean all the trailing spaces.${NONE}"
        echo -e "${BOLD}You can run 'scripts/check-trailing-spaces.sh --fix' for convenience.${NONE}"
        exit 1
    else
        echo
        echo -e "${GREEN}${BOLD}All trailing spaces have been cleaned.${NONE}"
        exit 0
    fi
else
    echo -e "${GREEN}${BOLD}No trailing spaces found.${NONE}"
    exit 0
fi
