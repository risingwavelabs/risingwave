#!/usr/bin/env bash

set -euo pipefail

ORIGINAL_BRANCH=$(git branch --show-current)

on_exit() {
  git checkout "$ORIGINAL_BRANCH"
}

trap on_exit EXIT

OLD_TAG=1.0.0
NEW_TAG=1.1.0

source backwards-compat-tests/scripts/utils.sh

setup_old_cluster() {
  echo "--- Setting up old cluster"
  git checkout "v${OLD_TAG}"
}

setup_new_cluster() {
  echo "--- Setting up new cluster"
  rm -r .risingwave/bin/risingwave
  git checkout main
}

main() {
  setup_old_cluster
  seed_old_cluster $OLD_TAG
  setup_new_cluster
  validate_new_cluster $NEW_TAG
}

main