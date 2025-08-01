# Memory (Heap) Profiling Guide

Note that the content below is Linux-exclusive.

## What is Heap Profile?

A heap profiler records the **stack trace of the allocation** of each **live** object, so it's possible that function A allocates something and then hand over it to struct B, in this case, the allocation will still be counted on A.

## Internals

RisingWave uses [tikv-jemallocator](https://crates.io/crates/tikv-jemallocator) on Linux, which is a Rust wrapper of [jemalloc](https://github.com/jemalloc/jemalloc), as its memory allocator. On other platforms, RisingWave uses the default allocator.

Luckily, jemalloc provides built-in profiling support ([official wiki](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Heap-Profiling)). jemallocator exposes the feature via a cargo feature ‘profiling’. [Here](https://gist.github.com/ordian/928dc2bd45022cddd547528f64db9174) is a simple guide to profiling with jemallocator.

For RisingWave, [feat: support heap profiling from risedev by fuyufjh · Pull Request #4871](https://github.com/risingwavelabs/risingwave/pull/4871) added all things needed. Please just follow the below steps.

## Step 1 - Collect Memory Profiling Dump

Depends on the deployment, click the corresponding section to read the instructions.

<details>
<summary>1.1. Profile RisingWave (locally) with risedev</summary>

Run a local cluster in EC2 instance with an additional environment variable `RISEDEV_ENABLE_HEAP_PROFILE`.

```shell
RISEDEV_ENABLE_HEAP_PROFILE=1 ./risedev d full
```

Under the hood, `risedev` set environment variable `MALLOC_CONF` for RisingWave process. [Here](https://github.com/risingwavelabs/risingwave/blob/8f1e6d8101344385c529d5ae2277b28160615e2c/src/risedevtool/src/task/compute_node_service.rs#L107) is the implementation.

By default, the profiler will output a profile result on every 4GB memory allocation. Running a query and waiting for a while, lots of `.heap` files will be generated in the current folder:

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

</details>


<details>
<summary>1.2. Profile RisingWave in testing pipelines</summary>

Currently, some testing pipelines such as longevity tests have enabled memory profiling by default, but some are not, such as performance benchmarks.

To enable heap profiling of compute nodes in benchmark pipelines, set environment variable when starting a job:

```
ENABLE_MEMORY_PROFILING=true
```

Under the hood, the pipeline script passes the value to kube-bench's parameter `benchmark.risingwave.compute.memory_profiling.enable` ([code here](https://github.com/risingwavelabs/risingwave-test/blob/cff0b432b15abd99ac4b13cf1db375d7d8907371/benchmarks/set_bench_configs.sh#L46), and then kube-bench sets the environment to RisingWave Pods ([code here](https://github.com/risingwavelabs/kube-bench/blob/5392fc8fba02fad044d6ce1eb35eb84cb22a029c/manifests/risingwave/risingwave.template.yaml#L124)).

Note that this is only for compute nodes. If you need to run profiling on other nodes, or need to tune the parameters of profiling, you may modify the parameters in risingwave-test's [env.override.toml](https://github.com/risingwavelabs/risingwave-test/blob/d3edb85a9cbc61b3848bc7f7920603a5b89d1e9b/benchmarks/ch-benchmark/env.override.toml) manually and run the job with that branch. ([Example](https://github.com/risingwavelabs/risingwave-test/commit/cff0b432b15abd99ac4b13cf1db375d7d8907371))

</details>

<details>
<summary>1.3. Profile RisingWave in Kubernetes/EKS</summary>

If you run into an OOM issue in Kukernetes, now you will need to enable memory profiling first and reproduce the problem.

To enable memory profiling, set the environment variables `MALLOC_CONF` to Pods.

```bash
# Example: `statefulsets` for CN and Meta
kubectl edit statefulsets/benchmark-risingwave-compute-c
# Example: `deployments` for other nodes
kubectl edit deployments/benchmark-risingwave-connector-c
```

Add the `MALLOC_CONF` env var. Note the `prof_prefix` is used to specify the path and file names of dump. By default, `/risingwave/cache/` is mounted to HostPath and will persist after Pod restarts, so we use it as dump path here.

```yaml
env:
- name: MALLOC_CONF
  value: prof:true,lg_prof_interval:38,lg_prof_sample:19,prof_prefix:/risingwave/cache/cn
```

The suggested values of `lg_prof_interval` are different for different nodes. See `risedev` code: [compactor_service](https://github.com/risingwavelabs/risingwave/blob/8f1e6d8101344385c529d5ae2277b28160615e2c/src/risedevtool/src/task/compactor_service.rs#L99), [compute_node_service.rs](https://github.com/risingwavelabs/risingwave/blob/8f1e6d8101344385c529d5ae2277b28160615e2c/src/risedevtool/src/task/compute_node_service.rs#L107), [meta_node_service.rs](https://github.com/risingwavelabs/risingwave/blob/8f1e6d8101344385c529d5ae2277b28160615e2c/src/risedevtool/src/task/meta_node_service.rs#L190).


Afterwards, the memory dump should be outputted to the specified folder. Use `kubectl cp` to download it to local.


</details>

<details>
<summary>1.4. Dump memory profile with risectl</summary>

You can manually dump a heap profiling with risectl for a compute node with Jemalloc profiling enabled (`MALLOC_CONF=prof:true`).

```shell
./risedev ctl profile heap --dir [dumped_file_dir]
```

The dumped files will be saved in the directory you specified.

Note: To profile compute nodes remotely, please make sure all remote nodes have a public IP address accessible from your local machine (where you are running `risedev`).

</details>



## Step 2 - Analyze with `jeprof`

Note that each of the `.heap` files are full snapshots instead of increments. Hence, simply pick the latest file (or any historical snapshot).

**jeprof** is a utility provided by jemalloc to analyze heap dump files. It reads both the executable binary and the heap dump to get a full heap profiling.

Note that the heap profiler dump file must be analyzed along with exactly the same binary that it generated from. If the memory dump is collected from Kubernetes, please refer to 2.2.

<details>
<summary>2.1. Use jeprof locally</summary>

`jeprof` is already compiled in jemallocator and should be compiled by cargo, use it as follows:

```shell
# find jeprof binary
find . -name 'jeprof'

# set execution permission
chmod +x ./target/release/build/tikv-jemalloc-sys-22f0d47d5c562226/out/build/bin/jeprof
```

**Faster `jeprof` (recommend)**

In some platforms `jeprof` runs very slow. The bottleneck is `addr2line`, if you want to speed up from 30 minutes to 3s, please use :

```shell
git clone https://github.com/gimli-rs/addr2line
cd addr2line
cargo b --examples -r
cp ./target/release/examples/addr2line <your-path>
```


</details>


<details>
<summary>2.2. Use jeprof in Docker images</summary>

`jeprof` is included in RisingWave image `v1.0.0` or later. For earlier versions, please copy an `jeprof` manually into the container.

Find a Linux machine and use `docker` command to start an environment with the specific RisingWave version. Here, `-v $(pwd):/dumps` mounts current directory to `/dumps` folder inside the container, so that you don't need to copy the files in and out.

```bash
docker run -it --rm --entrypoint /bin/bash -v $(pwd):/dumps  ghcr.io/risingwavelabs/risingwave:latest
```

</details>

Generate `collapsed` file.

```shell
jeprof --collapsed binary_file heap_file > heap_file.collapsed
```

For example:

```shell
jeprof --collapsed /risingwave/bin/risingwave jeprof.198272.123.i123.heap > jeprof.198272.123.i123.heap.collapsed
```

## Step 3 - Visualize Flame Graph

We recommend you to analyze `collapsed` file with [speedscope](https://www.speedscope.app/). Just drop the `.collapsed` file into it. Click `Left Heavy` in the top-left corner to merge shared calling stacks.

<details>
<summary>Alternative: Generate flame graph locally</summary>

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

</details>
