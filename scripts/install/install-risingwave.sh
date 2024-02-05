#!/bin/sh -e

OS=$(uname -s)
ARCH=$(uname -m)
VERSION=$(curl -s https://api.github.com/repos/risingwavelabs/risingwave/releases/latest | grep '.tag_name' | sed -E -n 's/.*(v[0-9]+.[0-9]+.[0-9])\",/\1/p')
BASE_URL="https://github.com/risingwavelabs/risingwave/releases/download"

# TODO(kwannoel): Add support for other OS and architectures.
if [[ "${OS}" == "Linux" && "${ARCH}" == "x86_64" || "${ARCH}" == "amd64" ]]; then
  BASE_ARCHIVE_NAME="risingwave-${VERSION}-x86_64-unknown-linux-all-in-one"
  ARCHIVE_NAME="${BASE_ARCHIVE_NAME}.tar.gz"
  URL="${BASE_URL}/${VERSION}/${ARCHIVE_NAME}"
  USE_BREW=0
elif [[ "${OS}" == "Darwin" ]] && [[ "${ARCH}" == "x86_64" || "${ARCH}" == "amd64" || "${ARCH}" == "aarch64" || "${ARCH}" == "arm64" ]]; then
  USE_BREW=1
else
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
if [[ "${USE_BREW}" -eq 1 ]]; then
  echo "Installing RisingWave using Homebrew."
  brew tap risingwavelabs/risingwave
  brew install risingwave
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
echo "  ./risingwave standalone"