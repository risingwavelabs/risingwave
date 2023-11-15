#!/usr/bin/env python3

import subprocess

TEST_MAP = {
    "test-notify": ["noelkwan", "noelkwan"],
    "e2e-iceberg-sink-tests": ["liurenjie"],
    "e2e-java-binding-tests": ["yiming"],
    "e2e-clickhouse-sink-tests": ["bohanzhang"],
    "e2e-pulsar-sink-tests": ["liurenjie"],
    "s3-source-test-for-opendal-fs-engine": ["congyiwang"],
    "pulsar-source-tests": ["liurenjie"],
    "connector-node-integration-test": ["siyuanwang"],
}

def get_failed_tests(get_test_status, test_map):
    failed_test_map = {}
    for test in test_map.keys():
        test_status = get_test_status(test)
        if test_status == "hard_failed" or test_status == "soft_failed":
            failed_test_map[test] = test_map[test]
    return failed_test_map

def generate_test_status_messages(failed_test_map):
    messages = []
    for test, users in failed_test_map.items():
        users = " ".join(map(lambda user: f"<@{user}>", users))
        messages.append(f"Test {test} failed {users}")
    return messages

def get_buildkite_test_status():
    return

def get_mock_test_status(test):
    mock_test_map = {
        "test-notify": "hard_failed",
        "e2e-iceberg-sink-tests": "passed",
        "e2e-java-binding-tests": "soft_failed",
        "e2e-clickhouse-sink-tests": "",
        "e2e-pulsar-sink-tests": "",
        "s3-source-test-for-opendal-fs-engine": "",
        "pulsar-source-tests": "",
        "connector-node-integration-test": ""
    }
    return mock_test_map[test]

def get_mock_test_status_all_pass(test):
    mock_test_map = {
        "test-notify": "hard_failed",
        "e2e-iceberg-sink-tests": "passed",
        "e2e-java-binding-tests": "soft_failed",
        "e2e-clickhouse-sink-tests": "",
        "e2e-pulsar-sink-tests": "",
        "s3-source-test-for-opendal-fs-engine": "",
        "pulsar-source-tests": "",
        "connector-node-integration-test": ""
    }
    return mock_test_map[test]

def format_cmd(messages):
    messages = "\\n".join(messages)
    if messages == "":
        cmd = "echo no failed tests"
    else:
        cmd=f"""
            cat <<- YAML | buildkite-agent pipeline upload 
            steps:
              - label: "notify test inner"
                command: echo "notify test"
                notify:
                  - slack:
                      channels:
                        - "#notification-buildkite"
                      message: {messages}
          YAML
          fi
        """
    return cmd

def run_test_1():
    failed_test_map = get_failed_tests(get_mock_test_status, TEST_MAP)
    messages = generate_test_status_messages(failed_test_map)
    print(messages)
    cmd = format_cmd(messages)
    print(cmd)

def run_test_2():
    failed_test_map = get_failed_tests(get_mock_test_status, TEST_MAP)
    messages = generate_test_status_messages(failed_test_map)
    print(messages)
    cmd = format_cmd(messages)
    print(cmd)

def main():
    failed_test_map = get_failed_tests(get_buildkite_test_status, TEST_MAP)
    messages = generate_test_status_messages(failed_test_map)

run_test()

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
