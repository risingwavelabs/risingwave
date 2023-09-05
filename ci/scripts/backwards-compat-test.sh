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
if [[ "$profile" != "ci-dev" ]] && [[ "$profile" != "ci-release" ]]; then
    echo "Invalid option: profile must be either ci-dev or ci-release" 1>&2
    exit 1
fi

source backwards-compat-tests/scripts/utils.sh

################################### Main

OLD_TAG=1.0.0
NEW_TAG=1.1.0

configure_rw() {
echo "--- Setting up cluster config"
cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: etcd
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: compactor
    - use: zookeeper
    - use: kafka
EOF

cat <<EOF > risedev-components.user.env
RISEDEV_CONFIGURED=true

ENABLE_MINIO=true
ENABLE_ETCD=true
ENABLE_KAFKA=true

# Fetch risingwave binary from release.
ENABLE_BUILD_RUST=false

# Ensure it will link the all-in-one binary from our release.
ENABLE_ALL_IN_ONE=true

# ENABLE_RELEASE_PROFILE=true
EOF
}

setup_old_cluster() {
  echo "--- Build risedev for $TAG, it may not be backwards compatible"
  git config --global --add safe.directory /risingwave
  git checkout "${TAG}-rc"
  cargo build -p risedev

  echo "--- Setup old release $TAG"
  pushd ..
  git clone --depth 1 --branch "${TAG}-rc" "https://github.com/risingwavelabs/risingwave.git"
  pushd risingwave
  mkdir -p target/debug
  echo "Branch:"
  git branch
  cp risingwave target/debug/risingwave

  echo "--- Start cluster on tag $TAG"
  git config --global --add safe.directory /risingwave
}

setup_new_cluster() {
  echo "--- Running Queries Old Cluster @ $TAG"
  run_sql_old_cluster

  echo "--- Kill cluster on tag $TAG"
  ./risedev k

  echo "--- Setup Risingwave @ $RW_COMMIT"
  download_and_prepare_rw $profile common
}

main() {
  setup_old_cluster
  configure_rw
  seed_old_cluster $OLD_TAG

  setup_new_cluster
  configure_rw
  validate_new_cluster $NEW_TAG
}

main