#!/usr/bin/env python3

import subprocess
import unittest
import os
import sys

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

We won't run the entire pipeline, only steps specified by the CI_STEPS environment variable.

For step (2), we need to check its outcome and only run the next step, if the outcome is successful.
'''


def format_step(env):
    commit = get_bisect_commit(env["GOOD_COMMIT"], env["BAD_COMMIT"])
    step = f'''
cat <<- YAML | buildkite-agent pipeline upload
steps:
  - label: "run-{commit}"
    key: "run-{commit}"
    trigger: "main-cron"
    soft_fail: true
    build:
      branch: {env["BISECT_BRANCH"]}
      commit: {commit}
      env:
        CI_STEPS: {env['CI_STEPS']}
  - wait
  - label: 'check'
    command: |
        GOOD_COMMIT={env['GOOD_COMMIT']} BAD_COMMIT={env['BAD_COMMIT']} BISECT_BRANCH={env['BISECT_BRANCH']} CI_STEPS=\'{env['CI_STEPS']}\' ci/scripts/find-regression.py check
YAML'''
    return step


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
def run_pipeline(env):
    step = format_step(env)
    print(f"--- running upload pipeline for step\n{step}")
    result = subprocess.run(step, shell=True)
    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)


# Number of commits for [start, end)
def get_number_of_commits(start, end):
    cmd = f"git rev-list --count {start}..{end}"
    result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"stderr: {result.stderr}")
        print(f"stdout: {result.stdout}")
        sys.exit(1)
    return int(result.stdout)


def get_bisect_commit(start, end):
    number_of_commits = get_number_of_commits(start, end)
    commit_offset = number_of_commits // 2
    if commit_offset == 0:
        return start

    cmd = f"git rev-list --reverse {start}..{end} | head -n {commit_offset} | tail -n 1"
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
        "GOOD_COMMIT": os.environ['GOOD_COMMIT'],
        "BAD_COMMIT": os.environ['BAD_COMMIT'],
        "BISECT_BRANCH": os.environ['BISECT_BRANCH'],
        "CI_STEPS": os.environ['CI_STEPS'],
    }

    print(f'''
GOOD_COMMIT={env["GOOD_COMMIT"]}
BAD_COMMIT={env["BAD_COMMIT"]}
BISECT_BRANCH={env["BISECT_BRANCH"]}
CI_STEPS={env["CI_STEPS"]}
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
        print("--- start bisecting")
        env = get_env()
        fetch_branch_commits(env["BISECT_BRANCH"])
        run_pipeline(env)
    elif cmd == "check":
        print("--- check pipeline outcome")
        env = get_env()
        fetch_branch_commits(env["BISECT_BRANCH"])
        commit = get_bisect_commit(env["GOOD_COMMIT"], env["BAD_COMMIT"])
        step = f"run-{commit}"
        cmd = f"buildkite-agent step get outcome --step {step}"
        outcome = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if outcome.returncode != 0:
            print(f"stderr: {outcome.stderr}")
            print(f"stdout: {outcome.stdout}")
            sys.exit(1)

        outcome = outcome.stdout.strip()
        if outcome == "soft_failed":
            print(f"commit failed: {commit}")
            env["BAD_COMMIT"] = commit
        elif outcome == "passed":
            print(f"commit passed: {commit}")
            env["GOOD_COMMIT"] = get_commit_after(env["BISECT_BRANCH"], commit)
        else:
            print(f"invalid outcome: {outcome}")
            sys.exit(1)

        if env["GOOD_COMMIT"] == env["BAD_COMMIT"]:
            report_step(env["GOOD_COMMIT"])
            return
        else:
            print(f"run next iteration, start: {env['GOOD_COMMIT']}, end: {env['BAD_COMMIT']}")
            run_pipeline(env)
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
            "GOOD_COMMIT": "72f70960226680e841a8fbdd09c79d74609f27a2",
            "BAD_COMMIT": "9ca415a9998a5e04e021c899fb66d93a17931d4f",
            "BISECT_BRANCH": "kwannoel/find-regress",
            "CI_STEPS": "test"
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
        GOOD_COMMIT=72f70960226680e841a8fbdd09c79d74609f27a2 BAD_COMMIT=9ca415a9998a5e04e021c899fb66d93a17931d4f BISECT_BRANCH=kwannoel/find-regress CI_STEPS='test' ci/scripts/find-regression.py check
YAML'''
        )


if __name__ == "__main__":
    # You can run tests by just doing ./ci/scripts/find-regression.py
    if len(sys.argv) == 1:
        unittest.main()
    else:
        main()
