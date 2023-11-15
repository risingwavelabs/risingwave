#!/usr/bin/env python3

TEST_OWNERS_MAP = {
    "test-notify": [],
    "e2e-iceberg-sink-tests": [],
    "e2e-java-binding-tests": [],
    "e2e-clickhouse-sink-tests": [],
    "e2e-pulsar-sink-tests": [],
    "s3-source-test-for-opendal-fs-engine": [],
    "pulsar-source-tests": [],
    "connector-node-integration-test-11": [],
    "connector-node-integration-test-17": []
}

def get_test_statuses(get_test_status, test_owners_map):
    for TEST_OWNER in TEST_OWNERS_MAP:

#
# OUTCOME=$(buildkite-agent step get "outcome" --step "test-notify")
# echo "outcome: $$OUTCOME"
#
# if [[ $$OUTCOME == "hard_failed" || $$OUTCOME == "soft_failed" ]]; then
# cat <<- YAML | buildkite-agent pipeline upload
# steps:
# - label: "notify test inner"
# command: echo "notify test"
# notify:
# - slack:
# channels:
# - "#notification-buildkite"
# message: "<@noelkwan> test"
# YAML
# fi
