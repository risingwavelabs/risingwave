#!/usr/bin/env bash

set -euo pipefail

ORIGINAL_BRANCH=$(git branch --show-current)

on_exit() {
  git checkout "$ORIGINAL_BRANCH"
}

trap on_exit EXIT

source backwards-compat-tests/scripts/utils.sh

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
RISEDEV_CONFIGURED=false

ENABLE_MINIO=true
ENABLE_ETCD=true
ENABLE_KAFKA=true

# Fetch risingwave binary from release.
ENABLE_BUILD_RUST=true

# Ensure it will link the all-in-one binary from our release.
ENABLE_ALL_IN_ONE=true

# ENABLE_RELEASE_PROFILE=true
EOF
}

setup_old_cluster() {
  echo "--- Setting up old cluster"
  LATEST_BRANCH=$(git branch --show-current)
  git checkout "v${OLD_VERSION}"
}

setup_new_cluster() {
  echo "--- Setting up new cluster"
  rm -r .risingwave/bin/risingwave
  git checkout $LATEST_BRANCH
}

main() {
  set -euo pipefail
  get_rw_versions
  setup_old_cluster
  configure_rw
  seed_old_cluster "$OLD_VERSION"

  setup_new_cluster
  configure_rw
  validate_new_cluster "$NEW_VERSION"
}

main