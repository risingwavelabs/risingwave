# RisingWave docs vs code compare workflow (sliced, parallel)

This directory contains a **parallelizable slicing plan** and **prompt templates** to compare:

- RisingWave codebase (this repo)
- RisingWave user docs repository: `risingwavelabs/risingwave-docs` (default: `main`)

## Files

- `slices.yml`: the slice manifest (docs scopes, code scopes, test signals).
- `prompt-claude-slice-template.md`: standardized prompt template for Claude Code.
- `run_claude_slices.py`: runner that executes slices in parallel via a Claude Code CLI command.
- `validate_slices.py`: drift validator for `slices.yml` (intended for periodic/CI use).

## Expected usage

1. Clone docs repo somewhere (example):

```bash
git clone https://github.com/risingwavelabs/risingwave-docs /path/to/risingwave-docs
```

2. Install runner deps (PyYAML):

```bash
python3 -m pip install -r tools/docs-code-compare/requirements.txt
```

3. Run the runner (it will render prompts and invoke Claude in parallel):

```bash
python3 tools/docs-code-compare/run_claude_slices.py \
  --docs-repo-root /path/to/risingwave-docs \
  --docs-repo-ref main \
  --product-version latest \
  --jobs 6
```

By default it calls `claude` and pipes the prompt via stdin. Override via:
- `--claude-cmd` and `--claude-args`, or env `CLAUDE_CMD` / `CLAUDE_ARGS`.

Notes for Claude Code CLI:
- For non-interactive runs, Claude Code expects **`--print`** (see `claude -h`).
- If you see `Invalid API key · Please run /login`, run `claude` and execute `/login` (interactive),
  or use `claude setup-token` to configure a long-lived token.
 - This runner enforces a safe default for automation:
   - `--print`
   - `--tools Read,Bash` (prevents write/edit prompts)
   - `--add-dir <code_repo_root>` and `--add-dir <docs_repo_root>`
   - `--permission-mode bypassPermissions` (to avoid hanging on permission prompts)
   You can still override by passing explicit `--claude-args`.

Dry run (only render prompts):

```bash
python3 tools/docs-code-compare/run_claude_slices.py \
  --docs-repo-root /path/to/risingwave-docs \
  --dry-run
```

Output layout:
- `tools/docs-code-compare/reports/run.json`: run metadata
- `tools/docs-code-compare/reports/results.json`: per-slice status and paths
- `tools/docs-code-compare/reports/<slice_id>/prompt.txt`
- `tools/docs-code-compare/reports/<slice_id>/slice_report.md` (stdout of Claude)

4. Alternatively, if you prefer manual execution, for each slice in `slices.yml`, run one Claude Code job
   using `prompt-claude-slice-template.md` with variables filled.

   Recommended variable rendering:
   - `{{docs_scopes}}`: a bullet list of absolute/relative paths under docs repo root
   - `{{code_scopes}}`: a bullet list of absolute/relative paths under code repo root
   - `{{test_signals}}`: a bullet list of paths (tests, examples, configs) used as evidence
   - `{{docs_repo_ref}}`: `main` (or a commit SHA) and let your script override it
   - `{{product_version}}`: `latest` (or your target version string)

5. Collect outputs as `slice_report.md` and aggregate them.

## Maintenance practices (periodic update of slices.yml)

Recommended lightweight practices:

- **Treat `slices.yml` as an index, not a spec**: keep slices stable; only adjust scopes when docs/code layout changes materially.
- **Add/adjust slices when these events happen**:
  - docs repo adds a new top-level topic directory (e.g., new integration area)
  - code repo adds a new major subsystem directory/crate (new surface area)
  - e2e/integration test structure changes (tests moved/renamed)
- **Automate drift detection**:
  - run `validate_slices.py` on a schedule (CI nightly or cron) to catch missing paths/typos early
  - optionally fail the job on warnings once the repo is stable (`--fail-on-warn`)

Validate against code only:

```bash
python3 tools/docs-code-compare/validate_slices.py
```

Validate against code + a local docs checkout:

```bash
python3 tools/docs-code-compare/validate_slices.py \
  --docs-repo-root /path/to/risingwave-docs
```
