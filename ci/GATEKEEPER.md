# Buildkite CI Security Gatekeeper

This directory contains the **security gatekeeper pipelines** for RisingWave's Buildkite CI. These pipelines implement a two-tier approval system for external contributor PRs to prevent supply chain attacks.

## Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   External PR   │────▶│   Gatekeeper     │────▶│   Full CI       │
│   (Fork)        │     │   (Approval)     │     │   (Tests)       │
└─────────────────┘     └──────────────────┘     └─────────────────┘
         │                       │                         │
         │                       ▼                         │
         │              ┌──────────────────┐              │
         │              │  Block Step      │              │
         └─────────────▶│  (Manual Review) │◀─────────────┘
                        └──────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │  Maintainer      │
                        │  Approves CI     │
                        └──────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `gatekeeper-pull-request.yml` | Security checkpoint for standard PR CI (`pull-request.yml`) |
| `gatekeeper-main-cron.yml` | Extended security checkpoint for main-cron tests |
| `GATEKEEPER.md` | This documentation |

## Configuration

### Step 1: Configure Buildkite Pipeline

1. Go to [Buildkite](https://buildkite.com/risingwavelabs)
2. Create a new Pipeline named `risingwave-pr-gatekeeper`
3. Connect to GitHub repository `risingwavelabs/risingwave`
4. In **Pipeline Settings**:
   - **Build pull requests**: ✅ Enabled
   - **Build pull requests from forks**: ✅ Enabled (required for external PRs)
   - **Build tags**: Optional
   - **Default branch**: `main`

### Step 2: Set Webhook

The GitHub webhook should point to the **gatekeeper pipeline**, not the actual CI pipeline:

```
Webhook URL: https://buildkite.com/webhook?token=xxx
             (pointing to gatekeeper)
```

### Step 3: Configure Steps (⚠️ SECURITY CRITICAL)

**⚠️ IMPORTANT SECURITY WARNING:**

The gatekeeper pipeline **MUST NOT** be loaded from the PR's repository checkout via `buildkite-agent pipeline upload`, because fork contributors could modify `ci/workflows/gatekeeper-pull-request.yml` to bypass the approval block or exfiltrate secrets.

**Correct Approach:**

In the Buildkite UI, define the **Steps** directly by **copying and pasting** the content of `ci/workflows/gatekeeper-pull-request.yml` from the trusted `main` branch:

1. Go to Pipeline Settings → Steps
2. Select "Edit Steps"
3. Copy the entire content of `ci/workflows/gatekeeper-pull-request.yml` from the `main` branch
4. Paste it into the Buildkite Steps editor
5. Save

**❌ DO NOT use:**
```yaml
# WRONG - This loads from PR checkout and is insecure!
steps:
  - label: ":pipeline: Upload Gatekeeper"
    command: buildkite-agent pipeline upload ci/workflows/gatekeeper-pull-request.yml
```

**✅ CORRECT - Paste content directly in Buildkite UI:**
- The gatekeeper pipeline definition is controlled by maintainers
- Cannot be modified by external contributors
- Updates must be done manually by maintainers in the Buildkite UI

**Maintenance:**
When updating the gatekeeper pipeline:
1. Update `ci/workflows/gatekeeper-pull-request.yml` in the repo (for version control)
2. Manually copy the updated content to Buildkite UI
3. Keep the repo version and UI version in sync

## Security Workflow

### For External Contributors (Fork PRs)

1. **Contributor opens PR** from forked repository
2. **GitHub webhook triggers** gatekeeper pipeline
3. **Gatekeeper detects fork** and creates a **Block Step**
4. **Maintainer reviews** code for security
5. **Maintainer clicks "Unblock"** in Buildkite UI
6. **Maintainer selects approval decision**:
   - ✅ **Approve** - Run full CI
   - ❌ **Reject** - Do not run CI
   - 📝 **Request Changes** - Wait for updates
7. **Gatekeeper uploads** the actual CI pipeline
8. **Full CI runs** with tests, builds, etc.

### For Internal Contributors (Branch PRs)

1. **Internal PR opened** from repository branch
2. **Gatekeeper detects internal** PR (no fork)
3. **CI uploads immediately** without blocking
4. **Full CI runs** automatically

## Approval Checklist

When reviewing external PRs, maintainers should check:

### Code Security
- [ ] No malicious code or backdoors
- [ ] No suspicious dependency changes
- [ ] No hardcoded secrets or tokens
- [ ] No crypto mining or resource abuse

### CI Security
- [ ] No modifications to `ci/workflows/`, `ci/scripts/`, or `.github/workflows/`
- [ ] No attempts to exfiltrate secrets
- [ ] No excessive resource consumption patterns

### Contributor Assessment
- [ ] GitHub profile appears legitimate
- [ ] Contribution history (if any) is reasonable
- [ ] PR description is clear and professional

## GitHub Actions Integration

The GitHub Actions workflow (`.github/workflows/external-pr-ci-status.yml`) provides:

- **Automatic labeling**: Adds `ci/external-pr` and `ci/pending-approval` labels
- **Status notifications**: Posts helpful comments on external PRs
- **Approval tracking**: Updates labels when CI is approved

## Labels

| Label | Meaning |
|-------|---------|
| `ci/external-pr` | PR is from a forked repository |
| `ci/pending-approval` | Waiting for maintainer CI approval |
| `ci/approved` | CI has been approved and is running |

## Troubleshooting

### CI Not Triggering for External PR
- Check if webhook is configured correctly
- Verify "Build pull requests from forks" is enabled in Buildkite
- Check gatekeeper pipeline logs for detection issues

### Approval Not Working
- Ensure the approver has write access to the Buildkite pipeline
- Check that the Block Step conditions are correct
- Verify `GITHUB_TOKEN` secret is configured

### False Positives (Internal PR marked as external)
- Check `BUILDKITE_PULL_REQUEST_REPO` vs `BUILDKITE_REPO` values
- The normalization logic may need adjustment for your GitHub Enterprise setup

## Audit Trail

All approvals are logged with:
- Timestamp
- PR number and author
- Approver identity
- Decision (approve/reject/request changes)
- Review notes (if provided)

Access these logs in the Buildkite job output or your logging system.

## Cost Impact

Based on recent statistics:
- External PRs: ~4% of all PRs
- Approval overhead: Minimal (one click)
- Resource savings: Prevents malicious usage of CI resources

## Contact

For questions or issues with the gatekeeper setup:
- Slack: `#ci-infrastructure` channel
- GitHub Issues: Tag with `area/ci` label
