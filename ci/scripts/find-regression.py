#!/usr/bin/env python3

import subprocess
import os
import sys
from common import *

def format_step(env, branch, commit, steps):
    print(f"Running pipeline on commit: {commit} with steps: {steps}")
    step=f'''
cat <<- YAML | buildkite-agent pipeline upload
steps:
  - label: "run-{commit}"
    key: "run-{commit}"
    trigger: "main-cron"
    build:
      branch: {branch}
      commit: {commit}
      env:
        CI_STEPS: {steps}
  - wait
  - label: 'check'
    command: |
      if [ $(buildkite-agent step get "outcome" --step "run-{commit}") == "hard_failed" ]; then
        START_COMMIT={env["START_COMMIT"]} END_COMMIT={env["END_COMMIT"]} BUILDKITE_BRANCH={env["BRANCH"]} BISECT_STEPS={env["BISECT_STEPS"]} ci/scripts/find-regression.py failed 
      else
        START_COMMIT={env["START_COMMIT"]} END_COMMIT={env["END_COMMIT"]} BUILDKITE_BRANCH={env["BRANCH"]} BISECT_STEPS={env["BISECT_STEPS"]} ci/scripts/find-regression.py passed 
      fi
YAML
        '''
    return step


# Triggers a buildkite job to run the pipeline on the given commit, with the specified tests.
def run_pipeline_on_commit(env, branch, commit, steps):
    step = format_step(env, branch, commit, steps)
    print(f"Running upload pipeline: step={step}")
    subprocess.run(step, shell=True)

def run(failing_test_key):
    test_map = get_test_map()
    current_build_commit = os.environ['BUILDKITE_COMMIT']
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

def main():
    env = {
        "START_COMMIT": os.environ['START_COMMIT'],
        "END_COMMIT": os.environ['END_COMMIT'],
        "BRANCH": os.environ['BUILDKITE_BRANCH'],
        "BISECT_STEPS": os.environ['BISECT_STEPS'],
        "STATE": sys.argv[1],
    }

    print(f'''
    START_COMMIT: {env["START_COMMIT"]}
    END_COMMIT: {env["END_COMMIT"]}
    BRANCH: {env["BRANCH"]}
    BISECT_STEPS: {env["BISECT_STEPS"]}
    STATE: {env["STATE"]}
    ''')

    if env["STATE"] == "start":
        print("start")
        run_pipeline_on_commit(env, "kwannoel/find-regress", "f0fa34cdeed95a08b2c7d8428a17d6de27b6588d", "e2e-test")
    elif env["STATE"] == "failed":
        print("failed")
    elif env["STATE"] == "passed":
        print("passed")
    else:
        print("Invalid type")
        sys.exit(1)


main()