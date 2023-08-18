# Risingwave All-in-One

## Playground

This mode is just for local development. It starts with an in-memory etcd store and in-memory hummock store.

## Monolithic Mode

This mode is for production. It provides cli parameters to configure etcd and object store.
It will spawn `meta`, `frontend` and `compute` node within a single process.
It will take cli parameters to configure etcd and object store, connector node, and the compactor node.

## Development Notes

The `cmd_all` module directly calls the entry points functions of `meta`, `frontend` and `compute` modules.
It does so within a single process.