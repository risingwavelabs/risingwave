#!/usr/bin/env bash
set -euo pipefail

TARGET_HOST="${RW_TARGET_HOST:?RW_TARGET_HOST is required}"
TARGET_PORT="${RW_TARGET_PORT:-4566}"
TARGET_DB="${RW_TARGET_DB:-dev}"
TARGET_USER="${RW_TARGET_USER:-root}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

psql \
  -h "${TARGET_HOST}" \
  -p "${TARGET_PORT}" \
  -U "${TARGET_USER}" \
  -d "${TARGET_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "${ROOT_DIR}/create_mv.sql"
