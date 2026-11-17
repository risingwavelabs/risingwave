### SF10 Q18 ColdRanges prefetch budget 4 versus 16 GiB, 2026-11-18

One guarded `risedev d full` run reused native release source
`0bb0df4acf388a958e9592787ec36ceebcb376ed` and executable SHA-256
`81fc8b116fcd1522f87eef68195d914ade84bfabfc2c0f29bc92eef56c0d78ed`.
The only TOML change from the preceding six-worker arm raised
`storage.prefetch_buffer_capacity_mb` from 4096 to 16384.
A startup-log check verified the override. The source SST fingerprint,
fresh cold v1346 APFS seed, SQL, plan, 53 actors, six LocalityProviders,
12 CN slots, fixed4, eight streaming workers, six dedicated I/O workers,
G128/P32/B16, N16/1 MiB, zero gap, Foyer cache settings and trace-off
state all matched. MultiGet, metadata reuse and in-flight reuse stayed
enabled. Foyer file caches were off. The backend was local MinIO.
The same binary required no additional compilation.

| Metric | Fixed release, 4 GiB, one run | Same release, 16 GiB, one run |
| --- | ---: | ---: |
| CREATE MV wall, s | 677.11 | 693.08 |
| Source checkpoint, s | 96.25 | 81.04 |
| Actual source complete from 1 s scrapes, s | 91.00–92.01 | 80.00–81.01 |
| Source GETs | 221,199 | 44,532 |
| After-source GETs | 83,208 | 78,409 |
| Total GETs / read GiB | 304,407 / 36.779 | 122,941 / 35.391 |
| Cumulative physical GET service, s | 717.847 | 269.720 |
| Physical GET mean / p99 / p99.9, ms | 2.358 / 16 / 32 | 2.194 / 16 / 32 |
| ObjectStore caller mean / p99 / p99.9, ms | 8.124 / 252.51 / 378.77 | 11.240 / 252.51 / 378.77 |
| After 5 s admission / dispatch p99, ms | 40.96 / 40.96 | 0.01 / 5.12 |
| Rejected prefetch window reservations | 199,920 | 0 |
| Peak sampled prefetch accounting, MiB | 4,096.00 | 11,364.69 |
| Peak CN RSS, MiB | 4,371 | 5,736 |
| Whole ordinary block outer fetches | 246,399 | 52,451 |
| Buffered-overlap Fill outer fetches | 6,328 | 513 |
| Partial-overlap physical dispatches | 111 | 300 |

The larger budget removed all observed full-window admission rejections.
Source GETs fell 79.9% and whole-query GETs fell 59.6%. After the first
five seconds, no sampled G or P limit was reached in the 16 GiB run.
Source physical GET service time fell from 597.34 to 152.07 aggregate
seconds across parallel reads. Source checkpoint latency improved by
15.21 s. The 16 GiB setting admits the conservative charge; observed peak
accounting was 11.10 GiB and peak CN RSS was 5.60 GiB.

The caller mean uses different request populations: the 4 GiB fallback
issued many more small logical reads. Its lower aggregate mean must be
interpreted alongside request count, size and admission/dispatch tails.
Percentiles are histogram bucket upper bounds. The timing results are
one run per budget.

The full-window rejection fallback still exists in the source. This
configuration isolates the no-rejection workload for further experiments.
The result establishes that the configured budget was sufficient for
this local input. Production budget sizing and reduced-window fallback
remain separate work.

#### Elapsed-time attribution

Higher prefetch capacity improved source scanning. Downstream processing
took longer in this particular run:

| Operator interval | 4 GiB wall / CPU, s | 16 GiB wall / CPU, s | Physical GET service, 4 → 16 GiB, s |
| --- | ---: | ---: | ---: |
| First LocalityProvider → provider 24 | 92.01 / 634 | 104.99 / 685 | 36.45 → 34.91 |
| Provider 24 → 22 | 256.00 / 1,063 | 268.06 / 1,131 | 46.17 → 44.02 |
| Provider 22 → 20 | 143.01 / 671 | 143.00 / 665 | 24.45 → 23.55 |
| Provider 20 → graph merge | 91.83 / 318 | 94.92 / 333 | 17.07 → 15.18 |

The first two downstream intervals added about 25 s with more CN CPU
work and less cumulative backend service. The first LocalityProvider
started 12.02 s earlier; the final graph merge completed 16.10 s later.
Table 38 iterator initialization grew from 806.68 to 846.96 cumulative
seconds, while candidate checks fell from 239.15M to 224.21M. Table 32
initialization decreased from 325.66 to 305.03 s. Intermediate state
is generated during each run. These observations locate the elapsed
difference in downstream CPU work; they do not isolate why that CPU
cost differed.

The old pre-fix six-worker measurement had 35,938 source GETs, 118,369
total GETs, and an 86.03 s source checkpoint. The 16 GiB fixed build
substantially restores IO aggregation, with a remaining source GET
difference relative to that historical implementation. The primary
same-binary comparison here is the fixed 4 versus 16 GiB pair.

All 136,472,104 snapshot rows were observed and all 100 output-oracle
rows matched. No measured read errors or retries occurred. Owned
services, listeners, full-profile containers and the disposable
MinIO/Postgres clone were removed. The lowest sampled free disk was
81.49 GiB. The original cold seed was retained.

The local archive is
`target/locality-tpch-q18/sf10-q18-cold-ranges-0bb0df4acf-io6-prefetch16g.t0813fud/`.
`prefetch16g-vs-4g.json` contains both phase reports, settings, binary
identity and new overlap observations. `prefetch16g-vs-4g.md` gives
the complete comparison table. Raw startup, congestion, source and
whole-query metrics and verified cleanup remain beside them.
