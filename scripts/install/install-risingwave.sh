#!/bin/sh -e

OS=$(uname -s)
ARCH=$(uname -m)
VERSION=$(curl -s https://api.github.com/repos/risingwavelabs/risingwave/releases/latest | jq '.tag_name' -r)
BASE_URL="https://github.com/risingwavelabs/risingwave/releases/download"

# TODO(kwannoel): Add support for other OS and architectures.
if [[ "${OS}" == "Linux" && "${ARCH}" == "x86_64" || "${ARCH}" == "amd64" ]]; then
  BASE_ARCHIVE_NAME="risingwave-${VERSION}-x86_64-unknown-linux-all-in-one"
else
  echo
  echo "Unsupported OS or Architecture: ${OS}-${ARCH}"
  echo
  echo "Supported OSes: Linux"
  echo "Supported architectures: x86_64"
  echo
  echo "Please open an issue at <https://github.com/risingwavelabs/risingwave/issues/new/choose>,"
  echo "if you would like to request support for your OS or architecture."
  echo
  exit 1
fi

ARCHIVE_NAME="${BASE_ARCHIVE_NAME}.tar.gz"
URL="${BASE_URL}/${VERSION}/${ARCHIVE_NAME}"

echo
echo "Downloading ${URL} into ${PWD}."
echo
curl -L "${URL}" | tar -zx || exit 1
chmod +x risingwave
echo
echo "Successfully downloaded the RisingWave binary, you can run it as:"
echo "  ./risingwave standalone"