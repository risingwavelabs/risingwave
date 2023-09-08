#!/bin/bash
set -ex

python3 /opt/feature-store/udf.py > /dev/null 2>&1&
sleep 5
lsof -i:8815  > /dev/null || {
  echo "failed to start udf"
  exit 1
}
sleep 5

psql -U root -h frontend-node-0 -p 4566 -d dev -a -f mfa-start.sql || {
  echo "failed to initialize db for mfa"
  exit 1
}
sleep 2

export GENERATOR_PATH=generator
python3 generator --types user --num-users=15 \
  --dump-users="$GENERATOR_PATH/users.json"
sleep 2

./feature-store-server --output-topics mfa > /opt/feature-store/.log/server_log &
RECOMMENDER_PID=$!
sleep 2
./feature-store-simulator --types mfa > /opt/feature-store/.log/simulator_log &
SIMULATOR_PID=$!

trap 'kill $SIMULATOR_PID; kill $RECOMMENDER_PID; kill $MODEL_PID' SIGINT
wait $SIMULATOR_PID
echo "Simulator finished"
wait $RECOMMENDER_PID
echo "Recommender finished"
wait $MODEL_PID
echo "Model finished"