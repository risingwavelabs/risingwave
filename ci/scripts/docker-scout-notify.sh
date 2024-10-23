#!/usr/bin/env bash

report=$(buildkite-agent meta-data get SCOUT_REPORT)

cat >> step.yaml << EOF
- label: "docker scout slack notification"
  notify:
    - slack:
        channels:
        - "#notification-buildkite"
        message: "Docker Scout Report\n${report}"
EOF

buildkite-agent pipeline upload step.yaml
