#!/usr/bin/env bash

################################### SCRIPT BOILERPLATE

set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

# profile is either ci-dev or ci-release
if [[ "$profile" == "ci-dev" ]]; then
  echo "Running in ci-dev mode"
elif [[ "$profile" == "ci-release" ]]; then
  echo "Running in ci-release mode"
else
    echo "Invalid option: profile must be either ci-dev or ci-release" 1>&2
    exit 1
fi

source e2e_test/backwards-compat-tests/scripts/utils.sh

################################### Main

configure_rw() {
VERSION="$1"
ENABLE_BUILD="$2"

echo "--- Setting up cluster config"
  if version_le "$VERSION" "1.8.9"; then
    cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: sqlite
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: compactor
EOF
  else
     # For versions >= 1.9.0, we have support for different sql meta-backend,
     # so we need to specify the meta-backend: sqlite
     cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
 steps:
   - use: minio
   - use: sqlite
   - use: meta-node
     meta-backend: sqlite
   - use: compute-node
   - use: frontend
   - use: compactor
EOF
  fi

cat <<EOF > risedev-components.user.env
RISEDEV_CONFIGURED=true

ENABLE_MINIO=true

# Whether to build or directly fetch binary from release.
ENABLE_BUILD_RUST=$ENABLE_BUILD

# Use target/debug for simplicity.
ENABLE_RELEASE_PROFILE=false
ENABLE_UDF=true
EOF

# See https://github.com/risingwavelabs/risingwave/pull/15448
if version_le "${VERSION:-}" "1.8.0" ; then
  echo "ENABLE_ALL_IN_ONE=true" >> risedev-components.user.env
fi
}

setup_old_cluster() {
  echo "--- Build risedev for $OLD_VERSION, it may not be backwards compatible"
  git config --global --add safe.directory /risingwave
  git checkout "v${OLD_VERSION}"
  cargo build -p risedev
  echo "--- Get RisingWave binary for $OLD_VERSION"
  OLD_URL=https://github.com/risingwavelabs/risingwave/releases/download/v${OLD_VERSION}/risingwave-v${OLD_VERSION}-x86_64-unknown-linux.tar.gz
  set +e
  wget --no-verbose "$OLD_URL"
  if [[ "$?" -ne 0 ]]; then
    set -e
    echo "Failed to download ${OLD_VERSION} from github releases, build from source later during \`risedev d\`"
    configure_rw "$OLD_VERSION" true
  elif [[ $OLD_VERSION = '1.10.0' || $OLD_VERSION = '1.10.1' || $OLD_VERSION = '1.10.2' || $OLD_VERSION = '2.0.0' ]]; then
    set -e
    echo "1.10.x, 2.0.0 have dynamically linked openssl, build from source later during \`risedev d\`"
    configure_rw "$OLD_VERSION" true
  else
    set -e
    tar -xvf risingwave-v"${OLD_VERSION}"-x86_64-unknown-linux.tar.gz
    mv risingwave target/debug/risingwave

    echo "--- Start cluster on tag $OLD_VERSION"
    git config --global --add safe.directory /risingwave
    configure_rw "$OLD_VERSION" false
  fi
}

setup_new_cluster() {
  echo "--- Setup Risingwave @ $RW_COMMIT"
  git checkout "$RW_COMMIT"
  download_and_prepare_rw "$profile" common
  # Make sure we always start w/o old config
  rm -r .risingwave/config
}

main() {
  set -euo pipefail
  # Make sure we have all the branches
  git fetch --all
  get_rw_versions

  setup_old_cluster
  seed_old_cluster "$OLD_VERSION"

  setup_new_cluster
  # Assume we use the latest version, so we just set to some large number.
  # The current $NEW_VERSION as of this change is 1.7.0, so we can't use that.
  # See: https://github.com/risingwavelabs/risingwave/pull/15448
  configure_rw "99.99.99" false
  validate_new_cluster "$NEW_VERSION"
}

main
