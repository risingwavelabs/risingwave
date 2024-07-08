#!/usr/bin/env python3

import subprocess
import os
import sys
from common import *

def run_test_1():
    test_map = get_test_map()
    failed_test_map = get_failed_tests(get_mock_test_status, test_map)
    message = generate_test_status_message(failed_test_map)
    if message == "":
        print("All tests passed, no need to notify")
        return
    else:
        print("Some tests failed, notify users")
        print(message)
        cmd = format_cmd(message)
        print(cmd)

# Gets the last passing build from the day before, on the main branch.
def get_last_passing_build_commit():
    return

def get_commits_between(last_passing_build_commit, current_build_commit):
    return


def format_step(branch, commit, steps):
    ci_steps = ",".join(steps)
    print(f"Running pipeline on commit: {commit} with steps: {steps}")
    step=f"""
cat <<- YAML | buildkite-agent pipeline upload
steps:
  - trigger: "main-cron"
    build:
      branch: {branch}
      commit: {commit}
      env:
        CI_STEPS: {steps}
YAML
        """
    return step


# Triggers a buildkite job to run the pipeline on the given commit, with the specified tests.
def run_pipeline_on_commit(branch, commit, steps):
    step = format_step(branch, commit, steps)
    print(f"Running upload pipeline: step={step}")
    subprocess.run(step, shell=True)

def run(failing_test_key):
    test_map = get_test_map()
    failed_test_map = get_failed_tests(get_buildkite_test_status, test_map)
    last_passing_build_commit = get_last_passing_build_commit()
    current_build_commit = os.environ['BUILDKITE_COMMIT']
    test_commits = get_commits_between(last_passing_build_commit, current_build_commit)
    start = 0
    end = len(test_commits) - 1 # Exclude the current commit
    test_commit = None
    result = None
    # binary search the commits
    while start < end:
        mid = (start + end) // 2
        test_commit = test_commits[mid]
        result = run_pipeline_on_commit(test_commit, failed_test_map)
        if result:
            start = mid + 1
        else:
            end = mid

    if test_commit is None:
        print("No regression found")
        return

    print(f"Regression found at commit {test_commit}")

# def main():
#     if len(sys.argv) < 2:
#         print("Usage: find-regression.py <failing_test_key>")
#         sys.exit(1)
#     failing_test_key = os.environ['FAILED_TEST_KEY']
#     run(failing_test_key)

def main():
    run_pipeline_on_commit("kwannoel/find-regress", "f0fa34cdeed95a08b2c7d8428a17d6de27b6588d", "e2e-test")

main()