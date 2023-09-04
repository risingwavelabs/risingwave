#!/usr/bin/env bash

set -euo pipefail

OLD_TAG=v0.18.0
NEW_TAG=main

source backwards-compat-tests/scripts/utils.sh

setup_old_cluster() {
  echo "--- Setting up old cluster"
  git checkout "$OLD_TAG"
}

setup_new_cluster() {
  echo "--- Setting up new cluster"
  git checkout "$NEW_TAG"
}

main() {
  setup_old_cluster
  seed_old_cluster
  setup_new_cluster
  validate_new_cluster
}

main