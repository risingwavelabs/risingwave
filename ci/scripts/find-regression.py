#!/usr/bin/env python3

import subprocess
import os
import sys
from common import *

'''
This script is used to find the commit that introduced a regression in the codebase.
It uses binary search to find the regressed commit.
It works as follows:
1. Use the start (inclusive) and end (exclusive) bounds, find the middle commit.
   e.g. given commit 0->1(start)->2->3->4(bad), start will be 1, end will be 4. Then the middle commit is (1+4)//2 = 2
        given commit 0->1(start)->2->3(bad)->4, start will be 1, end will be 3. Then the middle commit is (1+3)//2 = 2
        given commit 0->1(start)->2(bad), start will be 1, end will be 2. Then the middle commit is (1+2)//2 = 1.
        given commit 0->1(start,bad), start will be 1, end will be 1. We just return the bad commit (1) immediately.
2. Run the pipeline on the middle commit.
3. If the pipeline fails, the regression is in the first half of the commits. Recurse (start, mid)
4. If the pipeline passes, the regression is in the second half of the commits. Recurse (mid+1, end)
5. If start>=end, return start as the regressed commit.

We won't run the entire pipeline, only steps specified by the BISECT_STEPS environment variable.

For step (2), we need to check its outcome and only run the next step, if the outcome is successful.
'''

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