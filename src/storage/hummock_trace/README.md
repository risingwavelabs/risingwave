# Hummock trace and replay

## Tracing


### Risedev

Put this env variable in `risedev-components.user.env`
```toml
ENABLE_HM_TRACE=true
```
It makes `risingdev` put flag `hm_trace` in env variables.

You can also config written log path
```toml
HM_TRACE_PATH=".trace/hummock.ht"
```

Then running any risedev commands traces storage operations to the log file.

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

### Development
It's recommended to add `--cfg hm_trace` flag to `.cargo/config.toml` for development.
Example:
```toml
[target.'cfg(all())']
rustflags = [
  "--cfg",
  "hm_trace"
]
```

## Replay

### Config

Replaying requires the complete object storage from tracing. Please configure you object storage in your config file and make sure data remain in object storage.

Example:
```toml
[storage]
data_directory = "hummock_001"
local_object_store = "minio://hummockadmin:hummockadmin@host/hummock001"
```

### Run Replay

Default storage config file, it uses `src/config/risingwave.toml`
```
cargo run --package risingwave_hummock_test --bin replay -- --path <your-path-to-log-file>
```

Customized config file
```
cargo run --package risingwave_hummock_test --bin replay -- --path <your-path-to-log-file> --config <your-path-to-config>
```