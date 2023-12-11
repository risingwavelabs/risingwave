#!/bin/bash
set -x  # Enable printing of each command

# The model creation may take a long time. Our estimate is 30 seconds. But it can be longer in lower-perf machines.
sleep 30

QUERY='SELECT rental_price FROM home_rentals_model WHERE number_of_bathrooms = 2 AND sqft = 1000;'
psql -h localhost -p 55432 -U mindsdb -d mindsdb -c "$QUERY"