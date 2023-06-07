#!/usr/bin/env bash

set -euo pipefail

pushd ..
export SNAPSHOT_DIR=$PWD/sqlsmith-query-snapshots
set +u
if [[ ! -d $SNAPSHOT_DIR ]]; then
  echo "--- Cloning snapshots"
  git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/sqlsmith-query-snapshots.git
fi
set -u
popd

echo "source common utils"
source ci/scripts/common.sh

export ENABLE_RANDOM_SEED=1
export TEST_NUM=100
pushd src/tests/sqlsmith/scripts
echo "--- Running generation"
SNAPSHOT_DIR=$SNAPSHOT_DIR ./gen_queries.sh generate
popd