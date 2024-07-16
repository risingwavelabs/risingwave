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

## Main Cron Bisect Guide

1. Create a new build via buildkite: https://buildkite.com/risingwavelabs/main-cron/builds/#new
2. Add the following environment variables:
   - `START_COMMIT`: The lower bound (inclusive) of the regression range.
   - `END_COMMIT`: The commit hash of the bad commit we observed.
   - `BISECT_BRANCH`: The branch name where the bisect will be performed.
   - `BISECT_STEPS`: The `CI_STEPS` to run during the bisect. Separate multiple steps with a comma.
                     You can check the labels for this in `main-cron.yml`.
   - `CI_STEPS`: Just put `disable-build` here, this is used to skip other steps we are not interested in.

Example you can try on [buildkite](https://buildkite.com/risingwavelabs/main-cron/builds/#new):
- Branch: `kwannoel/find-regress`
- Environment variables:
  ```
  START_COMMIT=29791ddf16fdf2c2e83ad3a58215f434e610f89a
  END_COMMIT=7f36bf17c1d19a1e6b2cdb90491d3c08ae8b0004
  BISECT_BRANCH=kwannoel/test-bisect
  BISECT_STEPS="test-bisect,disable-build"
  CI_STEPS="disable-build"
  ```