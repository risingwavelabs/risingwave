#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

buildkite-agent meta-data get SCOUT_REPORT > scout.report
report=$(cat scout.report)
cat >> step.yaml << EOF
steps:
  - label: "docker scout slack notification"
    command: "echo '--- notify the scout report'"
    notify:
      - slack:
          channels:
            - "#notification-buildkite"
          message: |
            ${report}
EOF

buildkite-agent pipeline upload step.yaml
