#!/bin/bash
set -ex

psql -U root -h frontend-node-0 -p 4566 -d dev -a -f mfa-start.sql || {
  echo "failed to initialize db for mfa"
  exit 1
}
sleep 2

export GENERATOR_PATH=generator
python3 generator --types user --num-users=15 \
  --dump-users="$GENERATOR_PATH/users.json"
sleep 2

./feature-store-server > /opt/feature-store/.log/server_log &
RECOMMENDER_PID=$!
sleep 2
./feature-store-simulator > /opt/feature-store/.log/simulator_log &
SIMULATOR_PID=$!

trap 'kill $SIMULATOR_PID; kill $RECOMMENDER_PID; kill $MODEL_PID' SIGINT
wait $SIMULATOR_PID
echo "Simulator finished"
wait $RECOMMENDER_PID
echo "Recommender finished"
wait $MODEL_PID
echo "Model finished"