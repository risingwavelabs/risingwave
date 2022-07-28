#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
mkdir -p target/debug
buildkite-agent artifact download risingwave-dev target/debug/
buildkite-agent artifact download risedev-playground-dev target/debug/
mv target/debug/risingwave-dev target/debug/risingwave
mv target/debug/risedev-playground-dev target/debug/risedev-playground

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-playground

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev playground"
cargo make pre-start-playground
cargo make link-all-in-one-binaries

echo "debug stuff 1"
apt-get update
echo "debug stuff 2"
apt install apt-transport-https ca-certificates curl software-properties-common
echo "debug stuff 3"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
echo "debug stuff 4"
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu `lsb_release -cs` test"
echo "debug stuff 5"
apt update
echo "debug stuff 6"
apt install docker-ce
echo "debug stuff 7"
docker port mysql

echo "--- e2e test w/ Rust frontend - sink with mysql"
cargo make clean-data
cargo make ci-start

timeout 2m sqllogictest -p 4566 -d dev  './e2e_test/sink/**/*.slt'
