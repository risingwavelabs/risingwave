#!/bin/bash
set -ex

psql -U root -h frontend-node-0 -p 4566 -d dev -a -f taxi-start.sql || {
  echo "failed to initialize db for taxi"
  exit 1
}
sleep 2

python3 generator
sleep 2

python3 server/model  > /opt/feature-store/.log/model_log &
MODEL_PID=$!
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