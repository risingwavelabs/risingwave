# CPU Profiling Guide

Share an easy-to-use profiler and flamegraph tool: https://github.com/koute/not-perf.git

Record samples:

```shell
nperf record -p `pidof compute-node` -o perf.data`
```

Generate flamegraph: 

```shell
nperf flamegraph perf.data > perf.svg
```
