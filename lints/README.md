# Lints

Custom lints for RisingWave to enforce code style and best practices, empowered by `cargo dylint`.

See [cargo dylint](https://github.com/trailofbits/dylint) for more information.

## Install `cargo-dylint`

```bash
cargo install cargo-dylint dylint-link
```

## Run lints

To run all lints, run `cargo dylint --all` in the root of the repository.

If you find there are some compile errors, try updating the `cargo-dylint` binary by installing it again.

## Add new lints

To add a new lint, add a new file in the `src` directory to declare the lint, then register it in `fn register_lints(..)` in `lib.rs`.

## Test lints

To test a lint, create a new file in the `ui` directory and add it as an `example` in `Cargo.toml`. Add the test function in the corresponding lint file in `src` directory, then run `cargo test`.

## VS Code integration

Duplicate `.vscode/settings.json.example` to `.vscode/settings.json` to enable rust-analyzer integration for developing lints.

## Bump toolchain

The version of the toolchain is specified in `rust-toolchain` file under current directory.
It does not have to be exactly the same as the one used to build RisingWave, but it should be close enough to avoid compile errors.

The information below can be helpful in finding the appropriate version to bump to.

- The toolchain used by the latest version of `cargo-dylint`: https://github.com/trailofbits/dylint/blob/master/internal/template/rust-toolchain
- The toolchain used by the latest version of `clippy`: https://github.com/rust-lang/rust-clippy/blob/master/rust-toolchain
- The hash of the latest commit in `rust-lang/rust-clippy` repo for the dependency `clippy-utils`.

Run the lints after bumping the toolchain to verify it works.
