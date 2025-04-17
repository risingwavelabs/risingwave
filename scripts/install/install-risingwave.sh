#!/bin/sh -e

if [ -z "${OS}" ]; then
  OS=$(uname -s)
fi
if [ -z "${ARCH}" ]; then
  ARCH=$(uname -m)
fi
STATE_STORE_PATH="${HOME}/.risingwave/state_store"
META_STORE_PATH="${HOME}/.risingwave/meta_store"

VERSION=$(
  curl -sI 'https://github.com/risingwavelabs/risingwave/releases/latest' |
    grep -i "location:" | tr '\r' '\n' | awk -F '/' '{print $NF}'
)

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

############# BREW INSTALL
if [ "${USE_BREW}" -eq 1 ]; then
  echo "Installing RisingWave using Homebrew."
  brew tap risingwavelabs/risingwave
  brew install risingwave
  echo
  echo "Successfully installed RisingWave using Homebrew."
  echo
  echo "Run RisingWave:"
  echo
  echo "  risingwave"
  echo
  echo "Start a psql session:"
  echo
  echo "  psql -h localhost -p 4566 -d dev -U root"
  echo
  echo "To start a fresh cluster, you can just delete the data directory contents:"
  echo
  echo "  rm -r ~/.risingwave"
  echo
  echo "To view available options, run:"
  echo
  echo "  risingwave single-node --help"
  echo
  echo
  exit 0
fi

############# BINARY INSTALL
echo
echo "Downloading RisingWave@${VERSION} from ${URL} into ${PWD}."
echo
curl -L "${URL}" | tar -zx || exit 1
chmod +x risingwave
echo
echo "Successfully installed RisingWave@${VERSION} binary."
echo
echo "Run RisingWave:"
echo
echo "  ./risingwave"
echo
echo "Start a psql session:"
echo
echo "  psql -h localhost -p 4566 -d dev -U root"
echo
echo "To start a fresh cluster, you can just delete the data directory contents:"
echo
echo "  rm -r ~/.risingwave"
echo
echo "To view available options, run:"
echo
echo "  ./risingwave single-node --help"
echo
# Check if $JAVA_HOME is set, if not, prompt user to install Java, and set $JAVA_HOME.
if [ -z "${JAVA_HOME}" ]; then
  tput setaf 3
  echo "WARNING: Java is required to use RisingWave's Java Connectors (e.g. MySQL)."
  echo "Please install Java, and set the \$JAVA_HOME environment variable."
fi
