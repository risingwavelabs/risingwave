# Gatekeeper Deployment Guide

This guide walks through configuring Buildkite after the gatekeeper code is merged.

## Prerequisites

- Buildkite organization admin access
- GitHub repository admin access (for webhook configuration)
- AWS Secrets Manager access (for GITHUB_TOKEN)

---

## Step 1: Create Gatekeeper Pipeline in Buildkite

### 1.1 Create New Pipeline

1. Go to [Buildkite](https://buildkite.com/risingwavelabs)
2. Click **"New Pipeline"**
3. Configure:
   - **Name**: `risingwave-pr-gatekeeper`
   - **Description**: `Security gatekeeper for external PR approvals`
   - **Repository**: `git@github.com:risingwavelabs/risingwave.git`

### 1.2 Configure Pipeline Settings

Navigate to **Pipeline Settings** → **GitHub**:

```
☑️ Build pull requests
☑️ Build pull requests from forks          ← CRITICAL: Must be enabled
☑️ Skip builds with existing commits
☑️ Build pull request ready for review     (optional)
☐ Build tags                              (optional)
☐ Publish commit status                   (optional, may conflict)
☐ Publish blocked commits as comments     (optional)
```

Navigate to **Pipeline Settings** → **Builds**:

```
Branch Limiting:
  - main
  - release-*
  - (other protected branches if needed)
```

### 1.3 Configure Pipeline Steps

In **Pipeline Settings** → **Steps**, paste:

```yaml
steps:
  - label: ":pipeline: Upload Gatekeeper"
    command: buildkite-agent pipeline upload ci/workflows/gatekeeper-pull-request.yml
    plugins:
      - seek-oss/aws-sm#v2.3.2:
          env:
            GITHUB_TOKEN: github-token
```

**Note**: The `seek-oss/aws-sm#v2.3.2` plugin fetches secrets from AWS Secrets Manager.

---

## Step 2: Configure GitHub Webhook

### 2.1 Update Webhook URL

The GitHub webhook must point to the **gatekeeper pipeline**, not the old pipeline.

1. Go to GitHub Repository → **Settings** → **Webhooks**
2. Find the existing Buildkite webhook
3. **Either** update the existing webhook:
   - Change URL to the new gatekeeper pipeline webhook
   -
4. **Or** create a new webhook:
   - **Payload URL**: `https://webhook.buildkite.com/deliver/[token]`
   - **Content type**: `application/json`
   - **Events**: Select "Pull requests" and "Pushes"

### 2.2 Verify Webhook Events

Ensure these events are selected:
- ✅ Pull requests
- ✅ Pull request reviews
- ✅ Pull request review comments
- ✅ Pushes (for branch builds)

---

## Step 3: Configure Secrets

### 3.1 AWS Secrets Manager

The gatekeeper needs `GITHUB_TOKEN` to:
- Fetch PR information (author, labels)
- Post comments to GitHub PRs
- Update PR labels

**Add/Update Secret:**

```bash
# Secret name: github-token
# Secret value: <GitHub Personal Access Token>
# Permissions needed: repo, write:discussion
```

**Required Token Scopes:**
- `repo` - Access repository data
- `write:discussion` - Post comments
- `write:pull_requests` - Update labels (optional)

### 3.2 Verify Secret Access

Ensure the Buildkite agents have IAM permissions to read from AWS Secrets Manager:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:github-token*"
    }
  ]
}
```

---

## Step 4: Configure Labels (GitHub)

The gatekeeper and GitHub Actions use these labels:

Create them in your repository (if they don't exist):

| Label | Color | Description |
|-------|-------|-------------|
| `ci/external-pr` | `#FEF2C0` | PR is from a forked repository |
| `ci/pending-approval` | `#F9D0C4` | Waiting for maintainer CI approval |
| `ci/approved` | `#C2E0C6` | CI has been approved and is running |

**How to create:**
1. GitHub Repository → **Issues** → **Labels** → **New label**
2. Or use GitHub CLI:
   ```bash
   gh label create "ci/external-pr" --color FEF2C0 --description "PR is from a forked repository"
   gh label create "ci/pending-approval" --color F9D0C4 --description "Waiting for maintainer CI approval"
   gh label create "ci/approved" --color C2E0C6 --description "CI has been approved and is running"
   ```

---

## Step 5: Test the Deployment

### 5.1 Test Internal PR (Should Auto-Approve)

1. Create a branch in the main repository:
   ```bash
   git checkout -b test/internal-pr-gatekeeper
   echo "# Test" >> README.md
   git commit -am "test: verify gatekeeper for internal PR"
   git push origin test/internal-pr-gatekeeper
   ```

2. Open a PR to `main`

3. **Expected:**
   - Gatekeeper pipeline triggers
   - Detection step shows: `✅ Internal PR detected`
   - **No block step**
   - Full CI runs automatically

4. Verify in Buildkite logs:
   ```
   === PR Source Detection ===
   PR Repository: git://github.com/risingwavelabs/risingwave.git
   Main Repository: git://github.com/risingwavelabs/risingwave.git
   ✅ Internal PR detected
   === Result: IS_FORK=false ===
   ```

### 5.2 Test External PR (Should Require Approval)

1. Fork the repository to a personal account
2. Create a branch in the fork:
   ```bash
   git checkout -b test/external-pr-gatekeeper
   echo "# Test from fork" >> README.md
   git commit -am "test: verify gatekeeper for external PR"
   git push origin test/external-pr-gatekeeper
   ```

3. Open a PR from the fork to `main`

4. **Expected:**
   - Gatekeeper pipeline triggers
   - Detection step shows: `⚠️ Fork PR detected`
   - **Block step** appears
   - GitHub PR receives comment notification
   - Labels added: `ci/external-pr`, `ci/pending-approval`

5. Verify in Buildkite:
   - Pipeline shows "Security Review Required" block
   - Click "Unblock"
   - Select `✅ Approve - Run Full CI`
   - Click "Continue"

6. **After Approval:**
   - Full CI runs
   - GitHub comment updated
   - Label changes to `ci/approved`

### 5.3 Test Rejection Flow

1. Create another external PR from a fork
2. In Buildkite block step, select `❌ Reject - Do Not Run CI`
3. **Expected:**
   - Pipeline fails
   - CI does not run
   - PR shows failed check

---

## Step 6: Main-Cron Gatekeeper (Optional)

If you want to also protect main-cron tests:

### 6.1 Create Main-Cron Gatekeeper Pipeline

1. Create new pipeline: `risingwave-main-cron-gatekeeper`
2. Configure same GitHub settings as above
3. Set steps to:
   ```yaml
   steps:
     - label: ":pipeline: Upload Main-Cron Gatekeeper"
       command: buildkite-agent pipeline upload ci/workflows/gatekeeper-main-cron.yml
       plugins:
         - seek-oss/aws-sm#v2.3.2:
             env:
               GITHUB_TOKEN: github-token
   ```

### 6.2 Configure Label-Based Trigger

In your existing main-cron trigger workflow:
- Only trigger main-cron gatekeeper when `ci/main-cron/run-all` or `ci/main-cron/run-selected` labels are present

---

## Step 7: Team Training

### 7.1 For Maintainers (Who Approve CI)

Share this checklist:

```
🔒 External PR Approval Checklist

Before approving CI for a fork PR:

Code Security:
  □ No malicious code or backdoors
  □ No suspicious dependencies
  □ No hardcoded secrets
  □ No resource abuse (mining, etc.)

CI Security:
  □ No changes to ci/workflows/, ci/scripts/, .github/workflows/
  □ No attempts to exfiltrate secrets
  □ No excessive resource consumption

Contributor:
  □ GitHub profile looks legitimate
  □ PR description is clear

In Buildkite:
  1. Click "Unblock" on the block step
  2. Select: ✅ Approve - Run Full CI
  3. Add approval notes (optional)
  4. Click "Continue"
```

### 7.2 For Contributors (External)

Update CONTRIBUTING.md or PR template:

```markdown
## For External Contributors

If you're submitting a PR from a fork:
- CI requires manual approval from a maintainer
- This is for security and may take 24-48 hours
- You'll see "⏳ Pending Approval" status
- A maintainer will review and approve/reject
```

---

## Step 8: Monitoring & Alerts

### 8.1 Buildkite Notifications

Configure notifications for:
- Block steps waiting > 24 hours (escalation)
- Failed gatekeeper pipelines

### 8.2 Audit Logging

All approvals are logged in Buildkite job output. For enhanced auditing:

```yaml
# Optional: Add to gatekeeper upload step
- label: ":memo: Audit Log"
  command: |
    echo "{\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"pr\":\"$BUILDKITE_PULL_REQUEST\",\"approver\":\"$BUILDKITE_UNBLOCKER_NAME\",\"decision\":\"$BUILDKITE_UNBLOCKER_APPROVAL_DECISION\"}" >> /var/log/gatekeeper-audit.log
  soft_fail: true
```

---

## Rollback Plan

If issues occur:

### Emergency Bypass

Immediately update the gatekeeper pipeline steps to:

```yaml
steps:
  - label: ":warning: BYPASS GATEKEEPER (Emergency)"
    command: buildkite-agent pipeline upload ci/workflows/pull-request.yml
```

### Full Rollback

1. Disable the gatekeeper webhook
2. Re-enable the original pipeline webhook
3. Delete or pause the gatekeeper pipeline
4. Investigate and fix issues
5. Re-deploy when ready

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "CI Not Triggering for External PR" | Verify "Build pull requests from forks" is enabled |
| "GITHUB_TOKEN not found" | Check AWS Secrets Manager and IAM permissions |
| "Block Step Not Appearing" | Check `is_fork` meta-data value in logs |
| "False Positive (Internal marked as external)" | Verify URL normalization in detection logic |
| "GitHub comment not posted" | Check GITHUB_TOKEN has `repo` scope |

---

## Verification Checklist

After deployment, verify:

- [ ] Gatekeeper pipeline created in Buildkite
- [ ] GitHub webhook points to gatekeeper
- [ ] "Build pull requests from forks" enabled
- [ ] GITHUB_TOKEN configured in AWS Secrets Manager
- [ ] Labels created in GitHub
- [ ] Internal PR test passed (auto-approved)
- [ ] External PR test passed (required approval)
- [ ] Team trained on approval workflow
- [ ] Rollback plan documented

---

## Support

For deployment issues:
- Slack: `#ci-infrastructure`
- GitHub Issues: Tag with `area/ci`, `kind/deployment`
