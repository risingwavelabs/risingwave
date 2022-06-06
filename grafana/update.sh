#!/bin/bash

set -euo pipefail

./generate.sh

echo "$(tput setaf 4)Upload dashboard to localhost:3001$(tput sgr0)"

payload="{\"dashboard\": $(jq . risingwave-dashboard.json), \"overwrite\": true}"

curl -X POST \
  -H 'Content-Type: application/json' \
  -d "${payload}" \
  "http://admin:admin@localhost:3001/api/dashboards/db"
