# How this magic works

This crate is to configure the features of some dependencies: 
- [static log verbosity level](https://docs.rs/tracing/latest/tracing/level_filters/index.html#compile-time-filters). This is forced.
- static link some dependencies e.g., OpenSSL. This is optional and controlled by feature flag `rw-static-link`

If this crate is depended by another crate, Cargo's [feature unification](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification) will include the features used by this crate in the final binary.

It is (and should only be) depended by `risingwave_cmd` and `risingwave_cmd_all`, i.e., the final `risingwave` binaries.

It should not be depended by `workspace-hack`, otherwise the features will be always enabled.
