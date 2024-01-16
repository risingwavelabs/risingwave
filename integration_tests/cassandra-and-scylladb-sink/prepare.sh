#!/bin/bash

set -euo pipefail

# wait for cassandra and scylladb to start up
sleep 60

# setup cassandra
docker compose exec cassandra cqlsh -f prepare_cassandra_and_scylladb.sql

# setup scylladb
docker compose exec scylladb cqlsh -f prepare_cassandra_and_scylladb.sql
