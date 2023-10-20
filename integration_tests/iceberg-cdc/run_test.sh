#!/bin/bash

set -ex

# Start test environment.
docker-compose up -d

# Wait for the environment to be ready.
sleep 20;

# Generate data
docker build -t iceberg-cdc-datagen ../datagen
timeout 20 docker run --network=iceberg-cdc_default iceberg-cdc-datagen /datagen --mode clickstream --qps 1 mysql --user mysqluser --password mysqlpw --host mysql --port 3306 --db mydb &

cd python
poetry update --quiet
# Init source, mv, and sink.
poetry run python init.py
# Wait for sink to be finished.
sleep 60;
poetry run python check.py
