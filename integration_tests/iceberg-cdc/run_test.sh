#!/bin/bash

# Start test environment.
docker-compose up -d --wait

# To avoid exiting by unhealth, set it after start environment.
set -ex

# Generate data
docker build -t iceberg-cdc-datagen ../datagen
timeout 20 docker run --network=iceberg-cdc_default iceberg-cdc-datagen /datagen --mode clickstream --qps 1 mysql --user mysqluser --password mysqlpw --host mysql --port 3306 --db mydb &

cd python
poetry update --quiet
# Init source, mv, and sink.
poetry run python init.py
# Wait for sink to be finished.
sleep 40;
poetry run python check.py
