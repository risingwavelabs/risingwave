#!/bin/bash

set -ex

TABLE_NAME=home_rentals

# Check if the table exists in the database
if psql -h risingwave-standalone -p 4566 -U root -d dev -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $TABLE_NAME);"; then
    echo "Table $TABLE_NAME exists"
else
    echo "Table $TABLE_NAME does not exist"
    # Create a new table and insert records
    psql -h risingwave-standalone -p 4566 -U root -d dev -f ./prepare_risingwave.sql
fi

psql -h mindsdb -p 55432 -U mindsdb -d mindsdb -f ./prepare_mindsdb.sql
