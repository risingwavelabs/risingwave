# Micro Benchmarks

We have micro benchmarks for various components such as storage, stream and batch executors.

## Running Micro Benchmarks

You can run them by specifying their name.
For instance to run `json_parser` micro benchmark:

```shell
cargo bench json_parser
```

## Generating Flamegraph for Micro Benchmarks

Install [`cargo-flamegraph`](https://github.com/flamegraph-rs/flamegraph)

```shell
cargo install flamegraph
```

Run flamegraph + benchmark (change `json_parser` to whichever benchmark you want to run.)

```shell
cargo flamegraph --bench json_parser -- --bench
```