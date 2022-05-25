# Contribution guidelines

Thanks for your interest in contributing to RisingWave! We welcome and appreciate contributions.

This document describes how to submit your code changes. To learn about the development process, see the [Developer guide](docs/developer-guide.md). To understand the design and implementation of RisingWave, refer to the design docs listed in [readme.md](docs/README.md).

If you have questions, please [create a Github issue](https://github.com/singularity-data/risingwave/issues/new/choose) or ask in the RisingWave Community channel on Slack. Please use the [invitation link](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw) to join the channel.


## Table of contents

  - [Tests and miscellaneous checks](#misc-check)
  - [Submit a PR](#submit-a-pr)
    - [Pull Request title](#pull-request-title)
    - [Pull Request description](#pull-request-description)
    - [Sign the CLA](#sign-the-cla)
  - [Update CI workflow](#update-ci-workflow)
  - [Add new files](#add-new-files)
  - [Add new dependencies](#add-new-dependencies)
  - [Check in PRs from forks](#check-in-prs-from-forks)

## Tests and miscellaneous checks

Before submitting your code changes, ensure you fully test them and perform necessary checks. The testing instructions and necessary checks are detailed in the [developer guide](docs/developer-guide.md#test-your-code-changes).

## Submit a PR

### Pull Request title

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

### Pull Request description

- If your PR is small (such as a typo fix), you can go brief.
- If it is large and you have changed a lot, it's better to write more details.

### Sign the CLA

Contributors will need to sign Singularity Data's CLA.

## Update CI workflow

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

## Add new files

We use [skywalking-eyes](https://github.com/apache/skywalking-eyes) to manage license headers.
If you added new files, please follow the installation guide and run:

```shell
license-eye -c .licenserc.yaml header fix
```

## Add new dependencies

To avoid rebuild some common dependencies across different crates in workspace, we use
[cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/) to ensure all dependencies
are built with the same feature set across workspace. You'll need to run `cargo hakari generate`
after deps get updated.

Also, we use [cargo-udeps](https://github.com/est31/cargo-udeps) to find unused dependencies in
workspace.

We use [cargo-sort](https://crates.io/crates/cargo-sort) to ensure all deps are get sorted.

## Check in PRs from forks

```shell
gh pr checkout <PR id>
git checkout -b forks/<PR id>
git push origin HEAD -u
```

After that, CI checks will begin on branches of RisingWave's main repo,
and the status will be automatically updated to PRs from forks.
