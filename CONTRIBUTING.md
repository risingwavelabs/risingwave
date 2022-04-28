# Contribution and Development Guidelines

Thanks for your interest in contributing to RisingWave! We welcome and appreciate contributions.

For the design, implementation, and development tools of RisingWave, please refer to [RisingWave developer docs](https://github.com/singularity-data/risingwave/tree/main/docs).

If you have questions, please [create a Github issue](https://github.com/singularity-data/risingwave/issues/new/choose) or ask in the RisingWave Community channel on Slack. Please use the [invitation link](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw) to join the channel.

- [Contribution and Development Guidelines](#contribution-and-development-guidelines)
  - [Testing and Lint](#testing-and-lint)
    - [Lint](#lint)
    - [Unit Tests](#unit-tests)
    - [Planner Tests](#planner-tests)
    - [End-to-End Testing](#end-to-end-testing)
    - [End-to-End Testing on CI](#end-to-end-testing-on-ci)
  - [Misc Check](#misc-check)
  - [Submit a PR](#submit-a-pr)
    - [Pull Request Title](#pull-request-title)
    - [Pull Request Description](#pull-request-description)
    - [CLA](#cla)
  - [Update CI Workflow](#update-ci-workflow)
  - [When adding new files](#when-adding-new-files)
  - [When adding new dependencies](#when-adding-new-dependencies)
  - [To check-in PRs from forks](#to-check-in-prs-from-forks)

## Testing and Lint

The RisingWave project enforces several checks in CI. Everytime the code gets modified, several checks should be done.

### Lint

RisingWave requires all code to pass fmt, clippy, sort and hakari checks, which can be done in a single command:

```shell
./risedev install-tools # Install required tools for running unit tests
./risedev c             # Run all checks, shortcut for ./risedev check
```

### Unit Tests

RiseDev helps run unit tests fast with cargo-nextest. To run unit tests:

```shell
./risedev install-tools # Install required tools for running unit tests
./risedev test          # Run unit tests
```

If you want to see the coverage report,

```shell
./risedev test-cov
```

### Planner Tests

RisingWave's SQL frontend has some tests for plans of specific SQLs. See [Planner Test Guide](src/frontend/test_runner/README.md) for more information.

### End-to-End Testing

Currently, we use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to run RisingWave e2e tests.

sqllogictest installation is included when running `./risedev install-tools`. You may also install it with:

```shell
cargo install --git https://github.com/risinglightdb/sqllogictest-rs --features bin
```

To run end-to-end test, you will need to start a full cluster first:

```shell
./risedev d
```

Then run some e2e tests:

```shell
./risedev slt -p 4566 './e2e_test/v2/**/*.slt'
```

After running e2e tests, you may kill the cluster and clean data.

```shell
./risedev k  # shortcut for ./risedev kill
./risedev clean-data
```

As RisingWave's codebase is constantly changing, and all persistent data might not be in a stable format, if there's
some unexpected decode error, try `./risedev clean-data` first.

### End-to-End Testing on CI

As we are in the process of deprecating the legacy Java frontend, CI runs e2e tests with the legacy Java frontend.
We also have some special settings with environment variable to workaround some problems.

Basically, CI is using the following two configuration to run the full e2e test suite:

```shell
./risedev dev ci-3node
./risedev dev ci-1node
```

We may adjust the environment variable to enable some specific code to make all e2e tests pass. Refer to GitHub Action workflow for more information.

## Misc Check

For shell code, please run:

```shell
brew install shellcheck
shellcheck <new file>
```

For Protobufs, we rely on [buf](https://docs.buf.build/installation) for code formatting and linting. Please check out their documents for installation. To check if you violate the rule, please run the commands:

```shell
buf format -d --exit-code
buf lint
```

## Submit a PR

### Pull Request Title

As described in [here](https://github.com/commitizen/conventional-commit-types/blob/master/index.json), a valid PR title should begin with one of the following prefixes:

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `build`: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
- `ci`: Changes to RisingWave CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
- `chore`: Other changes that don't modify src or test files
- `revert`: Reverts a previous commit

For example, a PR title could be:

- `refactor: modify executor protobuf package path`
- `feat(execution): enable comparison between nullable data arrays`, where `(execution)` means that this PR mainly focuses on the execution component.

You may also check out previous PRs in the [PR list](https://github.com/singularity-data/risingwave/pulls).

### Pull Request Description

- If your PR is small (such as a typo fix), you can go brief.
- If it is large and you have changed a lot, it's better to write more details.

### CLA

Contributors will need to sign Singularity Data's CLA.

## Update CI Workflow

We use scripts to generate GitHub Action configurations based on templates in `.github/workflow-template`.

To edit the workflow files, you will need to install `yq` >= 4.16.

```shell
> brew install yq
> yq --version
yq (https://github.com/mikefarah/yq/) version 4.16.1
```

Then, you may edit the files in `workflow-template`.

* `template.yml` + `main-override.yml` = `main.yml`
* `template.yml` + `pr-override.yml` = `pull-request.yml`

After that, run `apply-ci-template` to update the final workflow config.

```shell
./risedev apply-ci-template
```

## When adding new files

We use [skywalking-eyes](https://github.com/apache/skywalking-eyes) to manage license headers.
If you added new files, please follow the installation guide and run:

```shell
license-eye -c .licenserc.yaml header fix
```

## When adding new dependencies

To avoid rebuild some common dependencies across different crates in workspace, we use
[cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/) to ensure all dependencies
are built with the same feature set across workspace. You'll need to run `cargo hakari generate`
after deps get updated.

Also, we use [cargo-udeps](https://github.com/est31/cargo-udeps) to find unused dependencies in
workspace.

We use [cargo-sort](https://crates.io/crates/cargo-sort) to ensure all deps are get sorted.

## To check-in PRs from forks

```shell
gh pr checkout <PR id>
git checkout -b forks/<PR id>
git push origin HEAD -u
```

After that, CI checks will begin on branches of RisingWave's main repo,
and the status will be automatically updated to PRs from forks.
