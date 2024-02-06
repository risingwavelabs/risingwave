#!/bin/sh -e

if [ -z "${OS}" ]; then
  OS=$(uname -s)
fi
if [ -z "${ARCH}" ]; then
  ARCH=$(uname -m)
fi

VERSION="v1.7.0-single-node"
# TODO(kwannoel): re-enable it once we have stable release in latest for single node mode.
#VERSION=$(curl -s https://api.github.com/repos/risingwavelabs/risingwave/releases/latest \
# | grep '.tag_name' \
# | sed -E -n 's/.*(v[0-9]+.[0-9]+.[0-9])\",/\1/p')
BASE_URL="https://github.com/risingwavelabs/risingwave/releases/download"

if [ "${OS}" = "Linux" ]; then
  if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ]; then
    BASE_ARCHIVE_NAME="risingwave-${VERSION}-x86_64-unknown-linux-all-in-one"
    ARCHIVE_NAME="${BASE_ARCHIVE_NAME}.tar.gz"
    URL="${BASE_URL}/${VERSION}/${ARCHIVE_NAME}"
    USE_BREW=0
  elif [ "${ARCH}" = "arm64" ] || [ "${ARCH}" = "aarch64" ]; then
    BASE_ARCHIVE_NAME="risingwave-${VERSION}-aarch64-unknown-linux-all-in-one"
    ARCHIVE_NAME="${BASE_ARCHIVE_NAME}.tar.gz"
    URL="${BASE_URL}/${VERSION}/${ARCHIVE_NAME}"
    USE_BREW=0
  fi
elif [ "${OS}" = "Darwin" ]; then
  if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ] || [ "${ARCH}" = "aarch64" ] || [ "${ARCH}" = "arm64" ]; then
     USE_BREW=1
  fi
fi

if [ -z "$USE_BREW" ]; then
  echo
  echo "Unsupported OS or Architecture: ${OS}-${ARCH}"
  echo
  echo "Supported OSes: Linux, macOS"
  echo "Supported architectures: x86_64"
  echo
  echo "Please open an issue at <https://github.com/risingwavelabs/risingwave/issues/new/choose>,"
  echo "if you would like to request support for your OS or architecture."
  echo
  exit 1
fi

############# Setup data directories
echo "Setting up data directories."
mkdir -p "${HOME}/.risingwave/data/state_store"
mkdir -p "${HOME}/.risingwave/data/meta_store"

############# BREW INSTALL
if [ "${USE_BREW}" -eq 1 ]; then
  echo "Installing RisingWave using Homebrew."
  brew tap risingwavelabs/risingwave
  brew install risingwave@${VERSION}
  echo "Successfully installed RisingWave using Homebrew."
  echo
  echo "You can run it as:"
  echo "  risingwave standalone"
  exit 0
fi

############# BINARY INSTALL
echo
echo "Downloading ${URL} into ${PWD}."
echo
curl -L "${URL}" | tar -zx || exit 1
chmod +x risingwave
echo
echo "Successfully downloaded the RisingWave binary, you can run it as:"
echo "  ./risingwave"
echo