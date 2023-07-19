# Micro Benchmarks

We have micro benchmarks for various components such as storage, stream and batch executors.

## Running Micro Benchmarks

You can run them by specifying their name.
For instance to run `json_parser` micro benchmark:

```shell
cargo bench json_parser
```

## Generating Flamegraph for Micro Benchmarks

> Note: Flamegraph generation depends on `perf`. You will need a linux box to run it.

1. Install [`cargo-flamegraph`](https://github.com/flamegraph-rs/flamegraph)
    ```shell
    cargo install flamegraph
    ```
   
2. Install `perf`. If on ubuntu:
   ```shell
   sudo apt install linux-tools
   ```
   
   If using EC2, you may need this instead:
   ```shell
   sudo apt install linux-tools-aws
   ```
   
   On an EC2 instance you may also need to set `paranoid` level to 1,
   to give the profiler necessary permissions.
   ```shell
   sudo sh -c  "echo 1 >/proc/sys/kernel/perf_event_paranoid"
   ```

3. Run flamegraph + benchmark (change `json_parser` to whichever benchmark you want to run.)
    ```shell
    cargo flamegraph --bench json_parser -- --bench
    ```
   
    Within a benchmark, there are also typically multiple benchmark groups.
    For instance, within the `json_parser` bench, there's `json_parser`, `debezium_json_parser_(create/read/update/delete)`
    To filter you can just append a regex. For instance to only bench `json_parser`:
    ```shell
    cargo flamegraph --bench json_parser -- --bench ^json_parser
    ```
   
    You can take a look at [Criterion Docs](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html)
    for more information.