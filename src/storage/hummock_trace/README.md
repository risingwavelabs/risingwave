# Hummock trace and replay

## Tracing


### Risedev

Put this env variable in `risedev-components.user.env`
```
ENABLE_HM_TRACE=true
```

It makes `risingdev` put flag `hm_trace` in env variables.

### CLI
If you wish to manually run `cargo` rather than `risedev`, set the env variable to enable tracing.
```
RUSTFLAGS="--cfg hm_trace --cfg tokio_unstable"
```
We must manually enable `tokio_unstable` because extra flag sources are mutually exclusive. If we provide this variable, cargo will not evaluate `build.rustflags` in `.cargo/config.toml`

For example, to start a traced playground

```
RUSTFLAGS="--cfg hm_trace --cfg tokio_unstable" cargo run --bin risingwave playground
```


## Replay

```
cargo run --package risingwave_hummock_test --bin replay -- --path <your-path-to-log-file>
```