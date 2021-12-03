Thanks for your interest in contributing to RisingWave! Contributions of many kinds are encouraged and most welcome.

If you have questions, please [create a Github issue](https://github.com/singularity-data/risingwave/issues/new/choose).

There are some tips for you.

## Code Formatting
Before submitting your pull request (PR), you should format the code first.

For Java code, please run:
```bash
cd java
./gradlew spotlessApply
```

For Rust code, please run:

```bash
cd rust
cargo fmt
cargo clippy --all-targets --all-features
```

For Protobufs, we rely on [prototool](https://github.com/uber/prototool#prototool-format) and [buf](https://docs.buf.build/installation) for code formatting and linting.
Please check out their documents for installation. To check if you violate the rule, please run the commands:

```bash
prototool format -d
buf lint
```

## Pull Request Title
As described in [here](https://github.com/commitizen/conventional-commit-types/blob/master/index.json), a valid PR title should begin with one of the following prefixes:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `build`: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
- `ci`: Changes to our CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
- `chore`: Other changes that don't modify src or test files
- `revert`: Reverts a previous commit

For example, a PR title could be:
- `refactor: modify executor protobuf package path`
- `feat(execution): enable comparison between nullable data arrays`, where `(execution)` means that this PR mainly focuses on the execution component.

You may also check out our previous PRs in the [PR list](https://github.com/singularity-data/risingwave/pulls).

## Pull Request Description
- If your PR is large and you have changed a lot, it's better for you to write a lot of details.
- If it is small (such as a typo fix), you can go brief.
