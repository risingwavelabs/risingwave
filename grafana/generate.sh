#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"

for dashboard_name in "risingwave-dev-dashboard" "risingwave-user-dashboard"; do
    # generate-dashboard is a cli tool supplied by grafanalib.
    # It should be setup in the virtual environment.
    generate-dashboard -o $dashboard_name.gen.json $dashboard_name.dashboard.py
    jq -c . $dashboard_name.gen.json > $dashboard_name.json
    cp $dashboard_name.json ../docker/dashboards/
done
