#!/usr/bin/env sh
# Format all JSON test-case files with jq (4-space indent, trailing newline).
# Usage: ./fmt-json.sh           format in place
#        ./fmt-json.sh --check   exit non-zero if any file is unformatted (CI)
set -eu
cd "$(dirname "$0")"
case "${1:-}" in --check) check=1 ;; *) check=0 ;; esac
rc=0
for f in tests/cases/*.json; do
	out=$(jq --indent 4 --ascii-output . "$f")
	if [ "$check" = 1 ]; then
		printf '%s\n' "$out" | diff -q - "$f" >/dev/null || { echo "unformatted: $f"; rc=1; }
	else
		printf '%s\n' "$out" >"$f"
	fi
done
exit $rc
