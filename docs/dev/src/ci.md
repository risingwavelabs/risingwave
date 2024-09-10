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

1. Create a new build via [buildkite](https://buildkite.com/risingwavelabs/main-cron-bisect/builds/#new)
2. Add the following environment variables:
   - `GOOD_COMMIT`: The good commit hash.
   - `BAD_COMMIT`: The bad commit hash.
   - `BISECT_BRANCH`: The branch name where the bisect will be performed.
   - `CI_STEPS`: The `CI_STEPS` to run during the bisect. Separate multiple steps with a comma.
     - You can check the labels for this in [main-cron.yml](https://github.com/risingwavelabs/risingwave/blob/main/ci/workflows/main-cron.yml),
       under the conditions for each step.

Example you can try on [buildkite](https://buildkite.com/risingwavelabs/main-cron-bisect/builds/#new):
- Environment variables:
  ```
  GOOD_COMMIT=29791ddf16fdf2c2e83ad3a58215f434e610f89a
  BAD_COMMIT=7f36bf17c1d19a1e6b2cdb90491d3c08ae8b0004
  BISECT_BRANCH=kwannoel/test-bisect
  CI_STEPS="test-bisect,disable-build"
  ```