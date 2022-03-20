Thanks for your interest in contributing to RisingWave! Contributions of many kinds are encouraged and most welcome.

If you have questions, please [create a Github issue](https://github.com/singularity-data/risingwave-dev/issues/new/choose).

There are some tips for you.

## Testing

We support both unit tests and end-to-end tests.

We recommend to install required tools prior to running steps in the following sections:

```shell
./risedev install-tools
```

### Unit Testing

To run unit tests for Rust, run:

```shell
./risedev test
```

To run unit tests for Java, run the following commands under the root directory:

```shell
make java_test
```

### End-to-End Testing

Currently, we use [sqllogictest-rs](https://github.com/singularity-data/sqllogictest-rs) to run our e2e tests.

To install sqllogictest:

```shell
cargo install --git https://github.com/risinglightdb/sqllogictest-rs --features bin
```

To run end-to-end tests with multiple compute-nodes, run the script:

```shell
./risedev dev ci-3node
sqllogictest -p 4567 -d dev './e2e_test/**/*.slt'
```

To run end-to-end tests with state store, run the script:

```shell
./risedev dev ci-1node
sqllogictest -p 4567 -d dev './e2e_test/**/*.slt'
```

It will start processes in the background. After testing, you can run the following scriptto clean-up:

```shell
./risedev k
./risedev clean-data
```

As our codebase is constantly changing, and all persistent data might not be in a stable format, if there's
some unexpected decode error, try `./risedev clean-data` first.

## Monitoring

Uncomment `grafana` and `prometheus` services in `risedev.yml`, and you can view the metrics.

## Tracing

Compute node supports streaming tracing. Tracing is not enabled by default, and you will need to
use `./risedev configure` to enable tracing components. After that, simply uncomment `jaeger`
service in `risedev.yml`.

## Dashboard

You may use RisingWave Dashboard to see actors in the system. It will be started along with meta node.

## Logging

The Java frontend uses `logback.xml` to configure its logging config.

The Rust components use `tokio-tracing` to handle both logging and tracing. The default log level is set as:

* Third-party libraries: warn
* Other libraries: debug

If you need to adjust log levels, simply change the logging filters in `utils/logging`.

## Code Formatting

Before submitting your pull request (PR), you should format the code first.

For Java code, please run:

```shell
cd java
./gradlew spotlessApply
```

For Rust code, please run:

```shell
./risedev check
```

If a new dependency is added to `Cargo.toml`, you may also run:

```shell
cargo sort -w
```

For shell code, please run:

```shell
brew install shellcheck
shellcheck <new file>
```

For Protobufs, we rely on [prototool](https://github.com/uber/prototool#prototool-format) and [buf](https://docs.buf.build/installation) for code formatting and linting. Please check out their documents for installation. To check if you violate the rule, please run the commands:

```shell
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

You may also check out our previous PRs in the [PR list](https://github.com/singularity-data/risingwave-dev/pulls).

## Pull Request Description

- If your PR is small (such as a typo fix), you can go brief.
- If it is large and you have changed a lot, it's better to write more details.

## GitHub Action

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

## When adding new files...

We use [skywalking-eyes](https://github.com/apache/skywalking-eyes) to manage license headers.
If you added new files, please follow the installation guide and run:

```
license-eye -c .licenserc.yaml header fix
```

## When adding new dependencies...

To avoid rebuild some common dependencies across different crates in workspace, we use
[cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/) to ensure all dependencies
are built with the same feature set across workspace. You'll need to run `cargo hakari generate`
after deps get updated.

Also, we use [cargo-udeps](https://github.com/est31/cargo-udeps) to find unused dependencies in
workspace.

We use [cargo-sort](https://crates.io/crates/cargo-sort) to ensure all deps are get sorted.
