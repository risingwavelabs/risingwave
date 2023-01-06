# CPU Profiling Guide

Share an easy-to-use profiler and flamegraph tool: https://github.com/koute/not-perf.git

1. Record samples: `nperf record -p `pidof compute-node` -o perf.data`
2. Generate flamegraph: `nperf flamegraph perf.data > perf.svg`

