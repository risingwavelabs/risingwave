#!/usr/bin/env bash

set -euo pipefail

./generate.sh

echo "$(tput setaf 4)Upload dashboard to localhost:3001$(tput sgr0)"

for dashboard in "risingwave-user-dashboard.json" "risingwave-dev-dashboard.json"; do
  payload="{\"dashboard\": $(jq . $dashboard), \"overwrite\": true}" 
  echo "$payload" > payload.txt
  curl -X POST \
    -H 'Content-Type: application/json' \
    -d @payload.txt \
    "http://admin:admin@localhost:3001/api/dashboards/db"

  rm payload.txt
done 



