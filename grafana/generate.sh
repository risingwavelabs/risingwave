#!/usr/bin/env bash

set -euo pipefail

for dashboard_name in "risingwave-dev-dashboard" "risingwave-user-dashboard"; do
    generate-dashboard -o $dashboard_name.gen.json $dashboard_name.dashboard.py
    python3 ./grafana_extract.py
    jq -c . $dashboard_name.gen.json > $dashboard_name.json
    cp $dashboard_name.json ../docker/dashboards/
done
