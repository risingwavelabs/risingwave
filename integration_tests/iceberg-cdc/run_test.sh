#!/bin/bash

# Start test environment.
docker-compose up -d --wait

# To avoid exiting by unhealth, set it after start environment.
set -ex

sleep 20
docker stop datagen

docker build -t iceberg-cdc-python python
docker run --network=iceberg-cdc_default iceberg-cdc-python
