#!/usr/bin/env python3

import subprocess

# Add new test keys here.
# Add their corresponding owners (by slack username) here.
# NOTE(kwannoel): we may have to migrate to use `slack_user_id`.
# I use `slack_username` since it is more readable, but not officially supported in the docs.
TEST_MAP = {
    "test-notify": ["noelkwan", "noelkwan"],
    "backfill-tests": ["noelkwan"],
    "e2e-iceberg-sink-tests": ["renjie"],
    "e2e-java-binding-tests": ["yiming"],
    "e2e-clickhouse-sink-tests": ["bohan"],
    "e2e-pulsar-sink-tests": ["renjie"],
    "s3-source-test-for-opendal-fs-engine": ["congyi"],
    "pulsar-source-tests": ["renjie"],
    "connector-node-integration-test": ["siyuan"],
}

def get_failed_tests(get_test_status, test_map):
    failed_test_map = {}
    for test in test_map.keys():
        test_status = get_test_status(test)
        if test_status == "hard_failed" or test_status == "soft_failed":
            failed_test_map[test] = test_map[test]
    return failed_test_map

def generate_test_status_message(failed_test_map):
    messages = []
    for test, users in failed_test_map.items():
        users = " ".join(map(lambda user: f"<@{user}>", users))
        messages.append(f"Test {test} failed {users}")
    message = "\\n".join(messages)
    return message

def get_buildkite_test_status(test):
    result = subprocess.run(f"buildkite-agent step get \"outcome\" --step \"{test}\"", capture_output = True, text = True, shell=True)
    outcome = result.stdout.strip()
    return outcome

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
    cmd=f"""
cat <<- YAML | buildkite-agent pipeline upload 
steps:
  - label: "Test"
    command: echo "running failed test notification" && exit 1
    notify:
      - slack:
          channels:
            - "#notification-buildkite"
          message: {messages}
YAML
        """
    return cmd

def run_test_1():
    failed_test_map = get_failed_tests(get_mock_test_status, TEST_MAP)
    message = generate_test_status_message(failed_test_map)
    if message == "":
        print("All tests passed, no need to notify")
        return
    else:
        print("Some tests failed, notify users")
        print(message)
        cmd = format_cmd(message)
        print(cmd)

def main():
    failed_test_map = get_failed_tests(get_buildkite_test_status, TEST_MAP)
    message = generate_test_status_message(failed_test_map)
    if message == "":
        print("All tests passed, no need to notify")
        return
    else:
        print("Some tests failed, notify users")
        print(message)
        cmd = format_cmd(message)
        print(cmd)
        subprocess.run(cmd, shell=True)
        print("notification sent")

main()