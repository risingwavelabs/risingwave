# Risingwave All-in-One

## Playground

This mode is just for local development. It starts with an in-memory etcd store and in-memory hummock store.

## Standalone Mode

This mode is for production. It provides cli parameters to configure etcd and object store.
It will spawn `meta`, `frontend` and `compute` node within a single process.

You can omit options, and the corresponding node will not be started in the standalone process:

```bash
standalone \
  --meta-opts="..." \
  --frontend-opts="..."
  # --compute-opts="..." not provided, so it won't be started.
```

### Examples of using standalone mode

You may run and reference the [demo script](../scripts/e2e-full-standalone-demo.sh), as an example.

### Internals

Standalone mode simply passes the options to the corresponding node, and starts them in the same process.

For example `--meta-opts` is parsed, and then Meta Node's entrypoint, `risingwave_meta_node::start`, is called with the parsed options.
If any option is missing, the corresponding node will not be started.