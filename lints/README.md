# Lints

Custom lints for RisingWave to enforce code style and best practices, empowered by `cargo dylint`.

See [cargo dylint](https://github.com/trailofbits/dylint) for more information.

## Run lints

To run all lints, run `cargo dylint --all` in the root of the repository.

## Add new lints

To add a new lint, add a new file in the `src` directory to declare the lint, then register it in `fn register_lints(..)` in `lib.rs`.

## Test lints

To test a lint, create a new file in the `ui` directory and add it as an `example` in `Cargo.toml`. Add the test function in the corresponding lint file in `src` directory, then run `cargo test`.

## VS Code integration

Duplicate `.vscode/settings.json.example` to `.vscode/settings.json` to enable rust-analyzer integration for developing lints.
