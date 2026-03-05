#!/bin/bash
# Test script for gatekeeper fork detection logic
# Usage: ./ci/scripts/test-gatekeeper.sh [PR_REPO] [MAIN_REPO]

set -e

# Test cases
run_test() {
    local test_name="$1"
    local pr_repo="$2"
    local main_repo="$3"
    local expected="$4"

    echo ""
    echo "=== Test: $test_name ==="
    echo "PR Repo: $pr_repo"
    echo "Main Repo: $main_repo"

    # Simulate the detection logic from gatekeeper.yml
    IS_FORK="false"
    if [[ -n "$pr_repo" ]]; then
        PR_REPO_NORMALIZED=$(echo "$pr_repo" | sed 's|https://||' | sed 's|git://||' | sed 's|github.com/||' | tr '[:upper:]' '[:lower:]')
        MAIN_REPO_NORMALIZED=$(echo "$main_repo" | sed 's|https://||' | sed 's|git://||' | sed 's|github.com/||' | tr '[:upper:]' '[:lower:]')

        if [[ "$PR_REPO_NORMALIZED" != "$MAIN_REPO_NORMALIZED" && "$PR_REPO_NORMALIZED" != "risingwavelabs/risingwave" ]]; then
            IS_FORK="true"
        fi
    fi

    echo "Result: IS_FORK=$IS_FORK"
    echo "Expected: $expected"

    if [[ "$IS_FORK" == "$expected" ]]; then
        echo "✅ PASS"
    else
        echo "❌ FAIL"
        return 1
    fi
}

echo "==================================="
echo "Gatekeeper Fork Detection Test Suite"
echo "==================================="

# Test 1: External fork
run_test \
    "External Fork (https)" \
    "https://github.com/external-user/risingwave" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "true"

# Test 2: Internal PR (git protocol)
run_test \
    "Internal PR (git protocol)" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "false"

# Test 3: Internal PR (https protocol)
run_test \
    "Internal PR (https protocol)" \
    "https://github.com/risingwavelabs/risingwave" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "false"

# Test 4: External fork (git protocol)
run_test \
    "External Fork (git protocol)" \
    "git://github.com/contributor/risingwave.git" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "true"

# Test 5: Empty PR repo (branch push)
run_test \
    "Branch Push (no PR)" \
    "" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "false"

# Test 6: Case sensitivity
run_test \
    "Case Sensitivity Test" \
    "https://github.com/RisingWaveLabs/risingwave" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "false"

# Test 7: Different repo name (should be fork)
run_test \
    "Different Repo Name" \
    "https://github.com/someone/forked-risingwave" \
    "git://github.com/risingwavelabs/risingwave.git" \
    "true"

echo ""
echo "==================================="
echo "Test Suite Complete"
echo "==================================="
