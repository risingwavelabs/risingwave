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
    echo "Usage: $self [-f|--fix]"
    echo
    echo "Options:"
    echo "  -f, --fix              Fix trailing spaces."
    echo "  -h, --help             Show this help message and exit."
}

fix=false
while [ $# -gt 0 ]; do
    case $1 in
    -f | --fix)
        fix=true
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

temp_file=$(mktemp)

echo -ne "${BLUE}"
git grep -nIP --untracked '[[:space:]]+$' -- ':!src/tests/regress/data' | tee $temp_file || true
echo -ne "${NONE}"

bad_files=$(cat $temp_file | cut -f1 -d ':' | sort -u)
rm $temp_file

if [ ! -z "$bad_files" ]; then
    if [[ $fix == true ]]; then
        for file in $bad_files; do
            sed -i '' -e's/[[:space:]]*$//' "$file"
        done

        echo
        echo -e "${GREEN}${BOLD}All trailing spaces listed above have been cleaned.${NONE}"
        exit 0
    else
        echo
        echo -e "${RED}${BOLD}Please clean all the trailing spaces listed above.${NONE}"
        echo -e "${BOLD}You can run '$self --fix' for convenience.${NONE}"
        exit 1
    fi
else
    echo -e "${GREEN}${BOLD}No trailing spaces found.${NONE}"
    exit 0
fi
