# Risingwave All-in-One

## Playground

This mode is just for local development. It starts with an in-memory etcd store and in-memory hummock store.

## Standalone Mode

This mode is for production. It provides cli parameters to configure etcd and object store.
It will spawn `meta`, `frontend` and `compute` node within a single process.
It will take cli parameters to configure etcd and object store, connector node, and the compactor node.

## Development Notes

The `cmd_all` module directly calls the entry points functions of `meta`, `frontend` and `compute` modules.
It does so within a single process.

You may take a look / run the [demo script](../scripts/e2e-full-standalone-demo.sh).

To run it, you can pass in the options for the relevant nodes.
You can omit nodes from being started in the standalone process by omitting their options.

```bash
  RUST_BACKTRACE=1 \
  cargo run -p risingwave_cmd_all \
            --profile "${RISINGWAVE_BUILD_PROFILE}" \
            ${RISINGWAVE_FEATURE_FLAGS} \
            -- standalone \
                 --meta-opts="..."
                 --frontend-opts="..."
                 # --compute-opts="..." not provided, so it won't be started.
```

Currently we only provide a standalone binary, without integration into `docker-compose` or `risedev` yet.

It will be integrated into both eventually.