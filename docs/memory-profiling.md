# Memory (Heap) Profiling Guide

Note that the content below is Linux-exclusive.

## What is Heap Profile?

A heap profiler records the **stack trace of the allocation** of each **live** object, so it’s possible that function A allocates something and then hand over it to struct B, in this case, the allocation will still be counted on A.

## Internals

RisingWave uses [tikv-jemallocator](https://crates.io/crates/tikv-jemallocator) on Linux, which is a Rust wrapper of [jemalloc](https://github.com/jemalloc/jemalloc), as its memory allocator. On other platforms, RisingWave uses the default allocator.

Luckily, jemalloc provides built-in profiling support ([official wiki](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Heap-Profiling)). jemallocator exposes the feature via a cargo feature ‘profiling’. [Here](https://gist.github.com/ordian/928dc2bd45022cddd547528f64db9174) is a simple guide to profiling with jemallocator.

For RisingWave, [feat: support heap profiling from risedev by fuyufjh · Pull Request #4871](https://github.com/risingwave-labs/risingwave/pull/4871) added all things needed. Please just follow the below steps.

## Step 1 - Deploy and Run

Run a local cluster in EC2 instance with an additional environment variable `RISEDEV_ENABLE_HEAP_PROFILE`.

```shell
RISEDEV_ENABLE_HEAP_PROFILE=1 ./risedev d full
```

Here we use `full` instead of `compose-3node-deploy` because `compose-3node-deploy` uses Docker container to run RisingWave processes, which makes it more difficult to do profiling and analyzing.

The profiler will output a profile result on every 4GB memory allocation. Running a query and waiting for a while, lots of `.heap` files will be generated in the current folder:

```
...
compactor.266308.15.i15.heap
compactor.266308.16.i16.heap
compactor.266308.17.i17.heap
compactor.266308.18.i18.heap
...
compute-node.266187.116.i116.heap
compute-node.266187.117.i117.heap
compute-node.266187.118.i118.heap
compute-node.266187.119.i119.heap
...
```

## Step 2 - Analyze with jeprof

Note that each of the `.heap` files are full snapshots instead of increments. Hence, simply pick the latest file (or any historical snapshot).

**jeprof** is a utility provided by jemalloc to analyze heap dump files. It reads both the executable binary and the heap dump to get a full heap profiling.

`jeprof` is already compiled in jemallocator and should be compiled by cargo, use it as follows:

```shell
# find jeprof binary
find . -name 'jeprof'

# set execution permission
chmod +x ./target/release/build/tikv-jemalloc-sys-22f0d47d5c562226/out/build/bin/jeprof
```

Then

```shell
jeprof --collapsed binary_file heap_file > heap_file.collapsed
```

For example:

```shell
./target/release/build/tikv-jemalloc-sys-22f0d47d5c562226/out/build/bin/jeprof --collapsed /home/ubuntu/risingwave/.risingwave/bin/risingwave/compute-node /home/ubuntu/risingwave/jeprof.198272.1283.i1283.heap > jeprof.198272.1283.i1283.collapsed
```

### Faster `jeprof` (recommended)

`jeprof` is very slow for large heap analysis, the bottleneck is `addr2line`, if you want to speed up from 30 minutes to 3s, please use :

```shell
git clone https://github.com/gimli-rs/addr2line
cd addr2line
cargo b --examples -r
cp ./target/release/examples/addr2line <your-path>
```


## Step 3 - Generate Flame Graph

This step can be either done on local machine or remote server.

Download and unarchive [FlameGraph](https://github.com/brendangregg/FlameGraph) utility.

Run

```shell
./flamegraph.pl --color=mem --countname=bytes heap_file.collapsed > flamegraph.svg
```

Example:

```shell
./flamegraph.pl --color=mem --countname=bytes jeprof.198272.4741.i4741.collapsed > flamegraph.svg
```

By the way, the step 2 and 3 can be written in one line with pipe:

```shell
jeprof --collapsed target/release/risingwave compute-node.10404.2466.i2466.heap | ~/FlameGraph/flamegraph.pl --color=mem --countname=bytes > flamegraph.svg
```
