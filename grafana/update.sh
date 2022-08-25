#!/bin/bash

set -euo pipefail

echo "$(tput setaf 4)Upload dashboard to localhost:3001$(tput sgr0)"

payload="{\"dashboard\": $(jq . risingwave-dashboard.json), \"overwrite\": true}" 

echo "$payload" > payload.txt


curl -X POST \
  -H 'Content-Type: application/json' \
  -d @payload.txt \
  "http://admin:admin@localhost:3001/api/dashboards/db"

rm payload.txt