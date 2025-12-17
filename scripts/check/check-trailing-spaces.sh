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
    echo "  -s, --skip-dir <dir>   Skip a directory (can be specified multiple times)."
    echo "                          Example: $self --skip-dir target --skip-dir src/tests"
    echo "  -h, --help             Show this help message and exit."
}

fix=false
skip_dirs=()
while [ $# -gt 0 ]; do
    case $1 in
    -f | --fix)
        fix=true
        ;;
    -s | --skip-dir)
        shift
        if [ $# -eq 0 ]; then
            echo -e "${RED}${BOLD}$self: missing argument for --skip-dir${NONE}"
            print_help
            exit 1
        fi
        skip_dirs+=("$1")
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
# Build git pathspec exclusions. `--` separates revs/options from pathspecs.
# Default: search repo root, but skip `.cargo/` and `src/tests/regress/`.
git_grep_pathspec=(-- ':' ':!.cargo' ':!src/tests/regress')
if [ ${#skip_dirs[@]} -gt 0 ]; then
    for d in "${skip_dirs[@]}"; do
        # Normalize leading "./" to keep the pathspec consistent.
        d="${d#./}"
        git_grep_pathspec+=(":!${d}")
    done
fi

git grep -nIP --untracked '[[:space:]]+$' "${git_grep_pathspec[@]}" | tee "$temp_file" || true
echo -ne "${NONE}"

bad_files=$(cat "$temp_file" | cut -f1 -d ':' | sort -u)
rm "$temp_file"

# Portable in-place sed (GNU vs BSD/macOS).
sed_inplace() {
    local expr=$1
    local file=$2
    if sed --version >/dev/null 2>&1; then
        # GNU sed
        sed -i -e "${expr}" "${file}"
    else
        # BSD/macOS sed
        sed -i '' -e "${expr}" "${file}"
    fi
}

if [ -n "$bad_files" ]; then
    if [[ $fix == true ]]; then
        while IFS= read -r file; do
            sed_inplace 's/[[:space:]]*$//' "$file"
        done <<< "$bad_files"

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
