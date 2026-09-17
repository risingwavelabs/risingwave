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

The version of the toolchain is specified in `rust-toolchain` file under current directory. It will be used to build the lints, and also be used by `dylint` to compile RisingWave, instead of the root-level `rust-toolchain`.

So the chosen toolchain needs to
1. be close enough to the root-level `rust-toolchain` to make RisingWave compile. It does not have to be exactly the same version though.
2. be close enough to the dependency `clippy_utils`'s corresponding `rust-toolchain` in the Clippy's repo.

(Note: `clippy_utils` depends on rustc's internal unstable API. When rustc has breaking changes, the `rust` repo's Clippy will be updated. And then it's [synced back to the Clippy repo bi-weekly](https://doc.rust-lang.org/clippy/development/infrastructure/sync.html#syncing-changes-between-clippy-and-rust-langrust). So ideally we can use `clippy_utils` in the rust repo corresponding to our root-level nightly version, but that repo is too large. Perhaps we can also consider copy the code out to workaround this problem.)

The information below can be helpful in finding the appropriate version to bump to.

- The toolchain used by the latest version of `cargo-dylint`: https://github.com/trailofbits/dylint/blob/master/internal/template/rust-toolchain
- The toolchain used by the latest version of `clippy`: https://github.com/rust-lang/rust-clippy/blob/master/rust-toolchain
- The hash of the latest commit in `rust-lang/rust-clippy` repo for the dependency `clippy-utils`.

Run the lints after bumping the toolchain to verify it works.
