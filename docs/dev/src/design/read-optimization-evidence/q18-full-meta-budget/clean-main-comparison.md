# Final SF10 Q18 configuration versus historical clean main

The final full run uses the 9 MiB budget in this directory. The
[pin instrumentation follow-up](../q18-meta-cache-pins/README.md)
covers only the recovered probe window. Both candidate budgets
used ColdRanges, the scheduler with six dedicated workers and
64 KiB gap allowance, native MultiGet, and meta reuse.

Three archived clean-main runs used source `88b9811433`, opt3
release, legacy Range with 4 GiB prefetch memory, and no scheduler.
All five full runs used the same v1346 source SST projection,
the same foreground SQL, six LocalityProviders, 53 actors,
fixed-4 job parallelism, eight streaming runtime workers, normal
compaction, and a matching 100-row oracle. Four advertised CN
slots were used for main and twelve for the candidate. Both
used twelve batch runtime threads. Intermediate SST layouts
varied under normal compaction. Backend GET statistics use
physical operation histograms and exclude scheduler waiting.

| Full-run metric | Clean main, median of 3 | Candidate 1 MiB | Candidate 9 MiB |
| --- | ---: | ---: | ---: |
| CREATE MV, s | 567.94 | 535.86 | 461.87 |
| Provider 24 to 22, s | 181.00 | 138.00 | 95.00 |
| CN CPU, s | 2,759 | 2,588 | 2,267 |
| CN GET count | 125,961 | 120,198 | 124,053 |
| CN read GiB | 37.36 | 36.27 | 36.39 |
| Backend GET mean, ms | 9.55 | 2.10 | 2.21 |
| Backend GET p99 bucket upper bound, ms | 256 | 16 | 16 |
| CN peak RSS, GiB | 6.020 | 5.612 | 5.602 |

The 9 MiB endpoint is 106.07 s (18.68%) quicker than the median
historical main run. The individual main runs took 567.94,
549.13 and 629.96 s. Relative to the median-wall run, the
candidate spent 39.02 s longer before provider 24, 83.00 s less
in the probe phase, and 62.05 s less after provider 22. The first
provider stage to provider 24 accounted for about 32.0 s of that
pre-probe increase. Phase medians in the table are independent.

This is an archived configuration comparison. The 74.0 s
difference between the candidate budgets comes from the separate
one-pair full run and includes the normal-compaction layout
differences. The frozen probe and pin experiments quantify the
retention mechanism on a matched SST layout. The high-concurrency
pin limit and recovered RSS attribution remain open.

[Per-run metrics, checksums and controls](clean-main-comparison.json)
record the five runs used in this comparison.
