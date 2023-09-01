#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

self=$0

# Shell colors
RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
BOLD='\033[1m'
NONE='\033[0m'

print_help() {
    echo "Usage: $self [-f|--fix] [-j|--parallel <parallel>]"
    echo
    echo "Options:"
    echo "  -f, --fix              Fix trailing spaces."
    echo "  -j, --parallel <parallel>"
    echo "                         Number of processes to run in parallel."
    echo "  -h, --help             Show this help message and exit."
}

do_check() {
    # The following is modified from https://github.com/raisedevs/find-trailing-whitespace/blob/restrict-to-plaintext-only/entrypoint.sh.

    files=$1
    has_trailing_spaces=false

    for file in $files; do
        lines=$(grep -E -rnIH "[[:space:]]+$" "$file" | cut -f-2 -d ":" || echo "")
        if [ ! -z "$lines" ]; then
            has_trailing_spaces=true
            if [[ $fix == true ]]; then
                sed -i '' -e's/[[:space:]]*$//' "$file"
            fi
            echo -e "${BLUE}$lines${NONE}"
        fi
    done

    if [[ $has_trailing_spaces == true ]]; then
        exit 1
    else
        exit 0
    fi
}

fix=false
parallel=$(nproc)
while [ $# -gt 0 ]; do
    case $1 in
    -f | --fix)
        fix=true
        ;;
    -j | --parallel)
        parallel=$2
        ;;
    -h | --help)
        print_help
        exit 0
        ;;
    *)
        echo -e "${RED}${BOLD}$self: invalid option \`$1\`\n${NONE}"
        print_help
        exit 1
        ;;
    esac
    shift
done

file_chunks=()
for i in $(seq 1 $parallel); do
    file_chunks+=("")
done

i=0
for file in $(git grep --cached -Il '' -- ':!src/tests/regress/data'); do
    file_chunks[$((i % parallel))]="${file_chunks[$((i % parallel))]} $file"
    ((i++))
done

pids=()
for chunk in "${file_chunks[@]}"; do
    do_check "$chunk" &
    pids+=($!)
done

n_found=0
for pid in "${pids[@]}"; do
    if ! wait $pids; then
        ((n_found++))
    fi
done

if [ $n_found -ne 0 ]; then
    if [[ $fix == false ]]; then
        echo
        echo -e "${RED}${BOLD}Please clean all the trailing spaces listed above.${NONE}"
        echo -e "${BOLD}You can run '$self --fix' for convenience.${NONE}"
        exit 1
    else
        echo
        echo -e "${GREEN}${BOLD}All trailing spaces listed above have been cleaned.${NONE}"
        exit 0
    fi
else
    echo -e "${GREEN}${BOLD}No trailing spaces found.${NONE}"
    exit 0
fi
