# Buildkite CI Triage Reference

## Prerequisites

- Prefer `bk` CLI with auth configured.
- Use `curl` fallback when `bk` auth is unavailable.
- Use `jq` for structured parsing.

Optional env:

```bash
export BUILDKITE_ORGANIZATION_SLUG=risingwavelabs
export BUILDKITE_API_TOKEN=<token>
```

## 1) Find failing Buildkite checks for a PR

```bash
gh pr checks <PR_NUMBER> --watch=false
```

Capture Buildkite URL(s):

```text
https://buildkite.com/<org>/<pipeline>/builds/<build_number>
```

## 2) Fetch build JSON

`bk`:

```bash
bk build view <build_number> --pipeline <pipeline> -o json > .context/bk_<build_number>.json
```

`curl`:

```bash
curl -sSL "https://buildkite.com/<org>/<pipeline>/builds/<build_number>.json" > .context/bk_<build_number>.json
```

## 3) List failed jobs

```bash
jq -r '.jobs[] | select(.exit_status!=null and .exit_status!=0) | [(.name // .step.label), (.exit_status|tostring), .id] | @tsv' .context/bk_<build_number>.json
```

## 4) Fetch job logs

`bk`:

```bash
bk job log <job_id> -p <pipeline> -b <build_number> --no-timestamps
```

`curl`:

```bash
curl -sSL "https://buildkite.com/organizations/<org>/pipelines/<pipeline>/builds/<build_number>/jobs/<job_id>/log" > .context/job_<job_id>_log.json
```

If needed, strip ANSI noise from terminal logs:

```bash
... | tr -cd '\11\12\15\40-\176' | perl -pe 's/\[[0-9;]*[mK]//g; s/\r//g'
```

High-signal patterns:

- `query result mismatch`
- `[Diff] (-expected|+actual)`
- `query is expected to fail with error:`
- `panic`
- `assertion failed`

## 5) Inspect artifacts when logs are insufficient

List artifacts:

```bash
bk artifacts list <build_number> -p <pipeline> --job <job_id> -o json
```

```bash
curl -sSL "https://buildkite.com/organizations/<org>/pipelines/<pipeline>/builds/<build_number>/jobs/<job_id>/artifacts"
```

Download by artifact id:

```bash
bk artifacts download <artifact_id>
```

```bash
curl -sSL "https://buildkite.com/organizations/<org>/pipelines/<pipeline>/builds/<build_number>/jobs/<job_id>/artifacts/<artifact_id>" -o /tmp/artifact
```

Common useful artifact files:

- `risedev-logs.zip`
- `risedev-logs/nodetype-*.log`

## 6) Common pitfalls

- `raw_log_url` can be null. Use job-id endpoint.
- Some `/download` URLs can 404 without auth. Use artifact endpoint URL and follow redirects.
- HTML pages are noisy and huge. Prefer JSON endpoints.
- `curl | head` can produce write errors; write to a file first.
- Some `bk` commands still need `BUILDKITE_ORGANIZATION_SLUG` even with `-p`.

## 7) Exit code hints

- `105`: usually wrapper/plugin non-zero in e2e/SLT jobs. Inspect underlying SLT logs.
- `4`: often deterministic simulation or recovery script failure.
- `143`: usually cancellation/termination (`SIGTERM`), often not a product regression by itself.
