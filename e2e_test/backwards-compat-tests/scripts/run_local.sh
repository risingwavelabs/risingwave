#!/usr/bin/env bash

set -euo pipefail

ORIGINAL_BRANCH=$(git branch --show-current)

on_exit() {
  git checkout "$ORIGINAL_BRANCH"
}

trap on_exit EXIT

source e2e_test/backwards-compat-tests/scripts/utils.sh

configure_rw() {
  VERSION="$1"

  echo "--- Setting up cluster config"
  if version_le "$VERSION" "1.9.0"; then
    cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: etcd
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: compactor
    - use: kafka
      user-managed: true
      address: message_queue
      port: 29092
EOF
  else
    cat <<EOF > risedev-profiles.user.yml
 full-without-monitoring:
   steps:
     - use: minio
     - use: etcd
     - use: meta-node
       meta-backend: etcd
     - use: compute-node
     - use: frontend
     - use: compactor
     - use: kafka
       user-managed: true
       address: message_queue
       port: 29092
EOF
  fi

cat <<EOF > risedev-components.user.env
RISEDEV_CONFIGURED=false

ENABLE_MINIO=true

# Fetch risingwave binary from release.
ENABLE_BUILD_RUST=true

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
  configure_rw "$OLD_VERSION"
  seed_old_cluster "$OLD_VERSION"

  setup_new_cluster
  configure_rw "99.99.99"
  validate_new_cluster "$NEW_VERSION"
}

main
