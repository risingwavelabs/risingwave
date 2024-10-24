#!/usr/bin/env bash

buildkite-agent meta-data get SCOUT_REPORT > scout.report
report=$(sed 's/^/            /g' scout.report)

cat >> step.yaml << EOF
steps:
  - label: "docker scout slack notification"
    command: "echo '--- notify the scout report'"
    notify:
      - slack:
          channels:
            - "#notification-buildkite"
          message: |
            Docker Scout Report
            ${report}
EOF

buildkite-agent pipeline upload step.yaml
