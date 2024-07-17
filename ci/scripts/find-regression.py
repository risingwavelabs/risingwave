#!/usr/bin/env python3

import subprocess
import unittest
import os
import sys
import time

'''
@kwannoel
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


def format_step(env, commit):
    step_name = f"run-{commit}"
    step = f'''
cat <<- YAML | buildkite-agent pipeline upload
steps:
  - label: "{step_name}"
    key: "{step_name}"
    trigger: "main-cron"
    soft_fail: true
    build:
      branch: {env["BISECT_BRANCH"]}
      commit: {commit}
      env:
        CI_STEPS: {env['BISECT_STEPS']}
YAML'''
    return step, step_name


def report_step(commit):
    step = f'''
cat <<- YAML | buildkite-agent pipeline upload
steps:
  - label: "Regressed Commit: {commit}"
    command: "echo 'Regressed Commit: {commit}'"
YAML'''
    print(f"--- reporting regression commit: {commit}")
    result = subprocess.run(step, shell=True)
    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)


# Triggers a buildkite job to run the pipeline on the given commit, with the specified tests.
def run_pipeline(env, commit):
    step, step_name = format_step(env, commit)
    print(f"--- running upload pipeline for step: {step_name}\n{step}")
    result = subprocess.run(step, shell=True)
    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)
    cmd = f"buildkite-agent step get outcome --job {step_name}"
    retries = 0
    max_retries = 10
    sleep_duration = 1
    while True:
        result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"--- failed to get outcome: {step_name}")
            print(f"stderr: {result.stderr}")
            print(f"stdout: {result.stdout}")
            retries += 1
            sleep_duration *= 2
            if retries > max_retries:
                print(f"--- exceeded max retries: {max_retries}")
                sys.exit(1)
            else:
                time.sleep(sleep_duration)
                continue

        outcome = result.stdout.strip()
        if outcome == "passed" or outcome == "soft_failed":
            return outcome
        else:
            print(f"--- step is still running: {step_name}, outcome: {outcome}")
            time.sleep(10)
            continue


# Number of commits for [start, end)
def get_number_of_commits(start, end):
    cmd = f"git rev-list --count {start}..{end}"
    result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)
    return int(result.stdout)


def get_bisect_commit():
    cmd = f"git rev-parse HEAD"
    result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)
    return result.stdout.strip()


def get_commit_after(branch, commit):
    cmd = f"git log --reverse --ancestry-path {commit}..origin/{branch} --format=%H | head -n 1"
    result = subprocess.run([cmd], shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)

    return result.stdout.strip()


def get_env():
    env = {
        "START_COMMIT": os.environ['START_COMMIT'],
        "END_COMMIT": os.environ['END_COMMIT'],
        "BISECT_BRANCH": os.environ['BISECT_BRANCH'],
        "BISECT_STEPS": os.environ['BISECT_STEPS'],
    }

    print(f'''
START_COMMIT={env["START_COMMIT"]}
END_COMMIT={env["END_COMMIT"]}
BISECT_BRANCH={env["BISECT_BRANCH"]}
BISECT_STEPS={env["BISECT_STEPS"]}
        ''')

    return env


def fetch_branch_commits(branch):
    cmd = f"git fetch -q origin {branch}"
    result = subprocess.run([cmd], shell=True)
    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)


def main():
    cmd = sys.argv[1]

    if cmd == "start":
        print("--- starting bisect")
        env = get_env()

        # Checkout the bisect branch
        print(f"--- checking out branch: {env['BISECT_BRANCH']}")
        fetch_branch_commits(env["BISECT_BRANCH"])
        cmd = f"git checkout {env['BISECT_BRANCH']} -q"
        subprocess.run([cmd], shell=True)

        # Start the bisect
        print(f"--- starting bisect: {env['END_COMMIT']}..{env['START_COMMIT']}")
        bisect_cmd = f"git bisect start {env['END_COMMIT']} {env['START_COMMIT']}"
        subprocess.run([bisect_cmd], shell=True)

        while True:
            commit = get_bisect_commit()
            step_result = run_pipeline(env, commit)

            print(f"--- {commit}: {step_result}")
            if step_result == "passed":
                mark_result_cmd = f"git bisect good"
            elif step_result == "soft_failed":
                mark_result_cmd = f"git bisect bad"
            else:
                print(f"invalid result: {step_result}")
                sys.exit(1)
            bisect_result = subprocess.run([mark_result_cmd], shell=True, capture_output=True, text=True)
            output = bisect_result.stdout.strip()
            if "is the first bad commit" in output:
                print(f"--- regressed commit: {commit}")
                cmd = f"git rev-parse HEAD"
                result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
                if result.returncode != 0:
                    print(f"stderr: {result.stderr}")
                    print(f"stdout: {result.stdout}")
                    sys.exit(1)
                print(f"--- regressed commit: {result.stdout.strip()}")
                return
            elif "revisions left to test after this" in output:
                print("--- bisecting further")
                continue
            else:
                print("--- bisect in bad state")
                print(f"stderr: {result.stderr}")
                print(f"stdout: {result.stdout}")
                sys.exit(1)

    else:
        print(f"invalid cmd: {cmd}")
        sys.exit(1)


# For the tests, we use RisingWave's sequence of commits, from earliest to latest:
# 617d23ddcac88ced87b96a2454c9217da0fe7915
# 72f70960226680e841a8fbdd09c79d74609f27a2
# 5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0
# 9ca415a9998a5e04e021c899fb66d93a17931d4f
class Test(unittest.TestCase):
    def test_get_commit_after(self):
        fetch_branch_commits("kwannoel/find-regress")
        commit = get_commit_after("kwannoel/find-regress", "72f70960226680e841a8fbdd09c79d74609f27a2")
        self.assertEqual(commit, "5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0")
        commit2 = get_commit_after("kwannoel/find-regress", "617d23ddcac88ced87b96a2454c9217da0fe7915")
        self.assertEqual(commit2, "72f70960226680e841a8fbdd09c79d74609f27a2")
        commit3 = get_commit_after("kwannoel/find-regress", "5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0")
        self.assertEqual(commit3, "9ca415a9998a5e04e021c899fb66d93a17931d4f")

    def test_get_number_of_commits(self):
        fetch_branch_commits("kwannoel/find-regress")
        n = get_number_of_commits("72f70960226680e841a8fbdd09c79d74609f27a2",
                                  "9ca415a9998a5e04e021c899fb66d93a17931d4f")
        self.assertEqual(n, 2)
        n2 = get_number_of_commits("617d23ddcac88ced87b96a2454c9217da0fe7915",
                                   "9ca415a9998a5e04e021c899fb66d93a17931d4f")
        self.assertEqual(n2, 3)
        n3 = get_number_of_commits("72f70960226680e841a8fbdd09c79d74609f27a2",
                                   "5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0")
        self.assertEqual(n3, 1)

    def test_get_bisect_commit(self):
        fetch_branch_commits("kwannoel/find-regress")
        commit = get_bisect_commit("72f70960226680e841a8fbdd09c79d74609f27a2",
                                   "9ca415a9998a5e04e021c899fb66d93a17931d4f")
        self.assertEqual(commit, "5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0")
        commit2 = get_bisect_commit("617d23ddcac88ced87b96a2454c9217da0fe7915",
                                    "9ca415a9998a5e04e021c899fb66d93a17931d4f")
        self.assertEqual(commit2, "72f70960226680e841a8fbdd09c79d74609f27a2")
        commit3 = get_bisect_commit("72f70960226680e841a8fbdd09c79d74609f27a2",
                                    "5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0")
        self.assertEqual(commit3, "72f70960226680e841a8fbdd09c79d74609f27a2")

    def test_format_step(self):
        fetch_branch_commits("kwannoel/find-regress")
        self.maxDiff = None
        env = {
            "START_COMMIT": "72f70960226680e841a8fbdd09c79d74609f27a2",
            "END_COMMIT": "9ca415a9998a5e04e021c899fb66d93a17931d4f",
            "BISECT_BRANCH": "kwannoel/find-regress",
            "BISECT_STEPS": "test"
        }
        step = format_step(env)
        self.assertEqual(
            step,
            '''
cat <<- YAML | buildkite-agent pipeline upload
steps:
  - label: "run-5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0"
    key: "run-5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0"
    trigger: "main-cron"
    soft_fail: true
    build:
      branch: kwannoel/find-regress
      commit: 5c7b556ea60d136c5bccf1b1f7e313d2f9c79ef0
      env:
        CI_STEPS: test
  - wait
  - label: 'check'
    command: |
        START_COMMIT=72f70960226680e841a8fbdd09c79d74609f27a2 END_COMMIT=9ca415a9998a5e04e021c899fb66d93a17931d4f BISECT_BRANCH=kwannoel/find-regress BISECT_STEPS='test' ci/scripts/find-regression.py check
YAML'''
        )


if __name__ == "__main__":
    # You can run tests by just doing ./ci/scripts/find-regression.py
    if len(sys.argv) == 1:
        unittest.main()
    else:
        main()
