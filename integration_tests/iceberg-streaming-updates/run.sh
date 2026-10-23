#!/usr/bin/env bash

set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
    printf '%s\n' \
        "Usage: bash integration_tests/iceberg-streaming-updates/run.sh [SQLLogicTest options]" \
        "Requires an existing RisingWave cluster, REST catalog, and S3-compatible storage." \
        "See README.md for ICEBERG_TEST_* environment variables."
    exit 0
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

for name in ICEBERG_TEST_CATALOG_URI ICEBERG_TEST_WAREHOUSE \
    ICEBERG_TEST_S3_ENDPOINT ICEBERG_TEST_S3_ACCESS_KEY ICEBERG_TEST_S3_SECRET_KEY; do
    if [[ -z "${!name:-}" ]]; then
        printf 'Required environment variable is missing: %s\n' "$name" >&2
        exit 1
    fi
done

export ICEBERG_TEST_PYTHON
ICEBERG_TEST_PYTHON="$(command -v "${ICEBERG_TEST_PYTHON:-python3}")"

case "${ICEBERG_TEST_FORMAT_VERSION:-}" in
    "") versions=(2 3) ;;
    2|3) versions=("$ICEBERG_TEST_FORMAT_VERSION") ;;
    *)
        printf 'ICEBERG_TEST_FORMAT_VERSION must be 2 or 3, or unset to run both.\n' >&2
        exit 1
        ;;
esac

for version in "${versions[@]}"; do
    export ICEBERG_TEST_FORMAT_VERSION="$version"
    ICEBERG_TEST_TABLE="rw_update_e2e_$("$ICEBERG_TEST_PYTHON" -c 'import uuid; print(uuid.uuid4().hex)')"
    export ICEBERG_TEST_TABLE
    printf 'Running Iceberg V%s integration test with table %s\n' "$version" "$ICEBERG_TEST_TABLE"
    if ./risedev slt -u root -j 1 "$@" \
        integration_tests/iceberg-streaming-updates/streaming_updates.slt; then
        continue
    else
        status=$?
        printf 'Iceberg V%s failed (exit %s). Preserving table %s and SQL objects for diagnosis.\n' \
            "$version" "$status" "$ICEBERG_TEST_TABLE" >&2
        exit "$status"
    fi
done
