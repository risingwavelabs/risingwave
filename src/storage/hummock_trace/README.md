# Hummock trace and replay

# Demo

## Step 0: Kill and clean running clusters
```bash
./risedev k
./risedev clean-data
```
Configure risedev to enable Hummock Trace
```
./risedev configure
```

## Step 1: Start a cluster with tracing
```bash
USE_HM_TRACE=true ./risedev d hummock-trace
```

## Step 2: Run an e2e test
```bash
./risedev slt-batch -p 4566 -d dev -j 1
```
After this, you will see data in the object storage under `.risingwave/data/minio/hummock001`.

## Step 3: Kill the cluster
The object storage only allows a single server to access it. We must kill the cluster before replaying.
```bash
./risedev k
```

## Step 4: Start MinIO
```bash
MINIO_ROOT_PASSWORD=hummockadmin \
MINIO_ROOT_USER=hummockadmin \
.risingwave/bin/minio server \
--address 127.0.0.1:9301 \
--console-address 127.0.0.1:9400 \
--config-dir .risingwave/config/minio \
.risingwave/data/minio
```

## Step 5: Replay
```bash
cargo run --package risingwave_hummock_test --bin replay -- \
--path .trace/hummock.ht \
--object-storage minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
--config src/config/hummock-trace.toml
```
We are all set!!

# Document
## Trace
### Config
There is a default config file in `src/config/hummock-trace.toml`.

In our config file, we must disable the vacuum of the compactor.
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
# Runtime tracing flag. False disables tracing even it is compiled(cfg set)
USE_HM_TRACE=true
```
It makes `risingdev` put flag `hm_trace` in env variables `RUSTFLAGS`.

Then running any risedev commands traces storage operations to the log file.

### CLI
You may use Cargo features to enable tracing.
Example:
```
cargo build -p risingwave_cmd -p risedev -p risingwave_storage --features hm-trace --profile dev --features rw-static-link
```

You may also use `risedev` to start tracing with profile "hm-trace".
Example:
```
USE_HM_TRACE=true ./risedev d hummock-trace
```
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


Default storage config file, it uses `src/config/risingwave.user.toml`
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