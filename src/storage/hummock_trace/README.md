# Hummock trace and replay

## Tracing

### Config
In your config file, we must disable the vacuum of the compactor.
```toml
[meta]
# Put a very large number
vacuum_interval_sec = 3000000
```
Otherwise, the compactor may remove data from the object storage.
### Risedev
We can run `risedev configure` to make a env file.

Put env variables in `risedev-components.user.env`
```toml
# Path of log file
HM_TRACE_PATH=".trace/hummock.ht"
# Runtime tracing flag. False disables tracing even it is compiled
USE_HM_TRACE=true
```
It makes `risingdev` put flag `hm_trace` in env variables `RUSTFLAGS`.

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
It's recommended to add `--cfg hm_trace` flag to `.cargo/config.toml` for development since Rust may compile everything again if we set RUSTFLAGS.

Example:
```toml
[target.'cfg(all())']
rustflags = [
  "--cfg",
  "hm_trace"
]
```

If we set the flag in root `Cargo.toml`, we don't need to set the env variable.

## Replay

### Config

Replaying requires the complete object storage from tracing. Please make sure data remain in object storage.

### Run Replay

Start a MinIO server before replaying.
Object storage data is usually stored in `.risingwave/data/minio` and config is stored in `.risingwave/config/minio`.
```
MINIO_ROOT_PASSWORD=hummockadmin MINIO_ROOT_USER=hummockadmin .risi
ngwave/bin/minio server --address 127.0.0.1:9301 --console-address 127
.0.0.1:9400 --config-dir .risingwave/config/minio .risingwave/data/minio
```


Default storage config file, it uses `src/config/risingwave.toml`
```
cargo run --package risingwave_hummock_test --bin replay --
--path <your-path-to-log-file>
--object-storage <your-object-storage>
```

Customized config file
```
cargo run --package risingwave_hummock_test --bin replay --
--path <your-path-to-log-file>
--config <your-path-to-config>
--object-storage <your-object-storage>
```