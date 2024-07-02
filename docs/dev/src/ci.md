# Continuous Integration

<!-- TODO: How to debug CI -->

## CI Labels Guide

- `[ci/run-xxx ...]`: Run additional steps in the PR workflow indicated by `ci/run-xxx` in your PR.
- `ci/pr/run-selected` + `[ci/run-xxx ...]` : Only run selected steps indicated by `ci/run-xxx` in your **DRAFT PR.**
- `ci/main-cron/run-all`: Run full `main-cron` workflow for your PR.
- `ci/main-cron/run-selected` + `[ci/run-xxx …]` : Run specific steps indicated by `ci/run-xxx`
  from the `main-cron` workflow, in your PR. Can use to verify some `main-cron` fix works as expected.
- To reference `[ci/run-xxx ...]` labels, you may look at steps from `pull-request.yml` and `main-cron.yml`.

### Example

<https://github.com/risingwavelabs/risingwave/pull/17197>

To run `e2e-test` and `e2e-source-test` for `main-cron` in your pull request:
1. Add `ci/run-e2e-test`.
2. Add `ci/run-e2e-source-tests`.
3. Add `ci/main-cron/run-selected` to skip all other steps which were not selected with `ci/run-xxx`.
