#!/usr/bin/env bash
set -euo pipefail

# Download and extract the ADBC Snowflake driver shared library from the official wheel release.
#
# This script is intentionally shared by:
# - RiseDev task `download-adbc-snowflake` (src/risedevtool/adbc.toml)
# - Docker image build (docker/Dockerfile)
#
# Inputs (env vars):
# - ADBC_VERSION: ADBC release version (default: 21)
# - ADBC_DRIVER_VERSION: driver version in wheel filename (default: 1.9.0)
# - DEST_DIR: where to place the extracted shared library (default: "${PWD}/.risingwave/bin/adbc")
# - TMP_DIR: temp directory for the downloaded wheel (default: "${PWD}/.risingwave/tmp")
#
# Outputs:
# - ${DEST_DIR}/libadbc_driver_snowflake.so (Linux) or .dylib (macOS)

ADBC_VERSION="${ADBC_VERSION:-21}"
ADBC_DRIVER_VERSION="${ADBC_DRIVER_VERSION:-1.9.0}"
DEST_DIR="${DEST_DIR:-"${PWD}/.risingwave/bin/adbc"}"
TMP_DIR="${TMP_DIR:-"${PWD}/.risingwave/tmp"}"

OS_TYPE="$(uname -s)"
ARCH_TYPE="$(uname -m)"

LIB_SUFFIX=""
WHEEL_SUFFIX=""

case "${OS_TYPE}" in
  Linux)
    LIB_SUFFIX="so"
    case "${ARCH_TYPE}" in
      x86_64)
        WHEEL_SUFFIX="manylinux1_x86_64.manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_5_x86_64"
        ;;
      aarch64)
        WHEEL_SUFFIX="manylinux2014_aarch64.manylinux_2_17_aarch64"
        ;;
      *)
        echo "Error: Unsupported Linux architecture: ${ARCH_TYPE}" >&2
        exit 1
        ;;
    esac
    ;;
  Darwin)
    LIB_SUFFIX="dylib"
    case "${ARCH_TYPE}" in
      x86_64)
        WHEEL_SUFFIX="macosx_10_15_x86_64"
        ;;
      arm64)
        WHEEL_SUFFIX="macosx_11_0_arm64"
        ;;
      *)
        echo "Error: Unsupported macOS architecture: ${ARCH_TYPE}" >&2
        exit 1
        ;;
    esac
    ;;
  *)
    echo "Error: Unsupported operating system: ${OS_TYPE}" >&2
    exit 1
    ;;
esac

DRIVER_NAME="libadbc_driver_snowflake.${LIB_SUFFIX}"
WHEEL_FILENAME="adbc_driver_snowflake-${ADBC_DRIVER_VERSION}-py3-none-${WHEEL_SUFFIX}.whl"
DOWNLOAD_URL="https://github.com/apache/arrow-adbc/releases/download/apache-arrow-adbc-${ADBC_VERSION}/${WHEEL_FILENAME}"

if [ -f "${DEST_DIR}/${DRIVER_NAME}" ]; then
  exit 0
fi

echo "ADBC Snowflake driver not found, downloading ADBC ${ADBC_VERSION} (driver ${ADBC_DRIVER_VERSION})"
echo "Platform: ${OS_TYPE} ${ARCH_TYPE}"
echo "Download URL: ${DOWNLOAD_URL}"

mkdir -p "${DEST_DIR}" "${TMP_DIR}"
WHEEL_PATH="${TMP_DIR}/${WHEEL_FILENAME}"
if ! curl -fL -o "${WHEEL_PATH}" "${DOWNLOAD_URL}"; then
  # The PyPI wheels below are byte-identical to the Apache ADBC 21 release assets.
  case "${ADBC_VERSION}:${ADBC_DRIVER_VERSION}:${OS_TYPE}:${ARCH_TYPE}" in
    21:1.9.0:Linux:x86_64)
      WHEEL_MIRROR_PATH="1f/f2/976f7a2717da0a59a8cc600de1f8ca4b46d8ac88f5b09b144317db3cc48d"
      WHEEL_SHA256="375d9f6e7cb66040ce7f96b3cdce199dfdd69c7622fd9575fbb6798b8f375cd2"
      ;;
    21:1.9.0:Linux:aarch64)
      WHEEL_MIRROR_PATH="eb/f8/f1b17d5b0c406b931908257fd4fc856fc1a6be0f737ca95d199db2f7ba07"
      WHEEL_SHA256="72a6f6d2ec99a75ddfb85b61d3ff56ff3c111feab690307975b74e5e90a1137f"
      ;;
    *)
      exit 1
      ;;
  esac

  echo "GitHub download failed, trying the matching wheel on PyPI"
  curl -fL --retry 3 -o "${WHEEL_PATH}" "https://files.pythonhosted.org/packages/${WHEEL_MIRROR_PATH}/${WHEEL_FILENAME}"
  printf '%s  %s\n' "${WHEEL_SHA256}" "${WHEEL_PATH}" | sha256sum -c -
fi

# Extract shared library from wheel (wheel is a zip file).
unzip -j -o "${WHEEL_PATH}" "adbc_driver_snowflake/${DRIVER_NAME}" -d "${DEST_DIR}"
rm -f "${WHEEL_PATH}"

if [ ! -f "${DEST_DIR}/${DRIVER_NAME}" ]; then
  echo "Error: ADBC Snowflake driver file not found after extraction" >&2
  echo "DEST_DIR contents:" >&2
  ls -la "${DEST_DIR}/" >&2 || true
  exit 1
fi

echo "ADBC Snowflake driver installed successfully at ${DEST_DIR}/${DRIVER_NAME}"
