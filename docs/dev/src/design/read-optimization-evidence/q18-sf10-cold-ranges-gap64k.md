### SF10 Q18 ColdRanges scheduler gap 0 versus 64 KiB, 2026-11-18

One guarded `risedev d full` arm reused source
`0bb0df4acf388a958e9592787ec36ceebcb376ed` and native release binary
SHA-256 `81fc8b116fcd1522f87eef68195d914ade84bfabfc2c0f29bc92eef56c0d78ed`.
The sole TOML change set scheduler `max_gap_bytes` from 0 to 65536.
Startup logs verified the override. The 1 MiB read ceiling, 16 GiB prefetch
budget, six dedicated I/O workers, eight streaming workers, G128/P32/B16,
N16/1 MiB prefetch windows, MultiGet, metadata reuse and in-flight reuse
matched the preceding zero-gap arm. A fresh APFS clone of the same
v1346 seed preserved the source SST fingerprint. SQL, plan, six
LocalityProviders, 53 actors, fixed4 and 12 CN slots matched. Both Foyer
file caches, tracing, iterator profiling and async stack tracing were off.
The backend was local MinIO. No compilation was needed.

The scheduler bridged some cache gaps. Source-checkpoint GETs fell
11.3%, with 79.91 MiB of explicitly counted scheduler overread. Source
physical and caller p99/p99.9 stayed in the same histogram buckets;
source average physical and caller latency increased about 5% and 6%.
The arm has zero prefetch-budget rejections.

| Metric | Zero scheduler gap | 64 KiB scheduler gap |
| --- | ---: | ---: |
| Source-checkpoint GETs | 44,532 | 39,504 |
| Source-checkpoint read GiB | 12.291 | 12.597 |
| Source scheduler overread MiB | 0 | 79.909 |
| Source merged logical members | 3,091 | 6,846 |
| Source in-flight followers | 108 | 56 |
| Source logical ObjectStore reads | 47,731 | 46,406 |
| Source physical GET mean / p99 / p99.9, ms | 3.415 / 32 / 64 | 3.585 / 32 / 64 |
| Source ObjectStore caller mean / p99 / p99.9, ms | 26.597 / 378.77 / 568.15 | 28.134 / 378.77 / 568.15 |
| Source admission p99 / p99.9, ms | 327.68 / 655.36 | 327.68 / 655.36 |
| Source dispatch p99 / p99.9, ms | 40.96 / 81.92 | 40.96 / 81.92 |
| Actual source completion, 1 s bracket, s | 80.00–81.01 | 81.00–82.01 |
| Source full-scrape checkpoint, s | 81.04 | 85.96 |
| Whole GETs / read GiB | 122,941 / 35.391 | 118,014 / 36.245 |
| Whole physical GET mean / p99 / p99.9, ms | 2.194 / 16 / 32 | 2.147 / 16 / 64 |
| Whole ObjectStore caller p99 / p99.9, ms | 252.51 / 378.77 | 378.77 / 378.77 |
| After 5 s admission / dispatch p99, ms | 0.01 / 5.12 | 0.01 / 5.12 |
| Peak CN RSS, MiB | 5,736 | 5,630 |
| CREATE MV wall, s | 693.08 | 658.97 |

Percentiles are histogram bucket upper bounds. Source measurements use
the retained full-source checkpoint convention. Whole-query tails have
mixed phase populations: the whole physical p99.9 and caller p99 landed
in higher buckets even though source tails stayed in the same buckets.
The pair contains one run per setting.

#### GET accounting and bytes

All source logical reads complete successfully and the physical identity
holds at both checkpoints. The 5,028 fewer source GETs decompose as:

| Change | Contribution to physical GET difference |
| --- | ---: |
| Fewer logical ObjectStore reads | -1,325 |
| Additional members merged by scheduler | -3,755 |
| Fewer reads served by in-flight reuse | +52 |
| Total | -5,028 |

The cold-gap count also changed from 8,485 to 6,752. Declared cold
segments fell from 46,708 to 45,351, while nonempty windows increased
from 38,223 to 38,599. Scheduler timing and cache publication alter the
upstream window shape, so the GET drop cannot all be assigned to an
unchanged set of gap pairs. The positive 83,790,197-byte overread
counter establishes that physical cross-gap reads occurred.

At the measured source checkpoints, read volume rose 312.94 MiB
(2.49%), exceeding the 79.91 MiB explicitly charged to scheduler gaps.
Other changes in submitted spans, sharing and measurement-boundary
work contribute to that difference. The additional bytes should be
reported using both counters rather than attributing the entire source
byte increase to gap reads.

Relative to the historical pre-fix six-worker 35,938 source GETs, the
zero-gap fixed run had 8,594 additional GETs. This arm recovers 5,028
of them (58.5%), leaving 3,566 (9.9% above pre-fix). This is a historical
endpoint comparison in addition to the primary same-binary gap pair.
The retained aggregate counters cannot classify every remaining
unmerged gap by simultaneous queue residency, span cap, or arrival time.

#### Source measurement boundary

The zero-gap checkpoint precedes its first LocalityProvider. The 64 KiB
checkpoint at 85.96 s follows first-provider startup at 83.09 s.
The latest pre-provider full scrape, at 80.92 s, has 135,795,245 of
136,472,104 source rows (99.50%), 39,006 GETs and 12,701.66 MiB read.
There are another 498 GETs and 197.71 MiB before the completion checkpoint,
covering remaining source work and downstream startup. Thus pure source
GETs lie between 39,006 and 39,504, and their reduction versus the
complete zero-gap source is between 11.3% and 12.4%. The upper source
read estimate includes some downstream startup. The one-second source
row series places actual completion at 81–82 s, rather than 85.96 s.
No additional run is needed to establish this sampling-boundary limit.

#### Whole-query elapsed time

The provider 24→22 interval fell from 268.06 to 224.01 s. Table 38 SST
candidate checks fell from 224.21M to 194.25M and CN CPU time from
1,131 to 964 CPU-s. Cumulative physical GET service in that phase moved
from 44.02 to 43.05 s. The provider 22→20 interval increased from
143.00 to 153.00 s as table 32 candidates rose from 78.11M to 90.82M.
These phases account for about 34 s of the 34.11 s whole-query decrease.
Intermediate SST state is generated during the query. Whole elapsed
improvement therefore remains confounded by downstream work/layout;
physical overread and merged-member counters provide the direct
scheduler evidence in this comparison.

All 136,472,104 source rows and all 100 independent oracle rows matched.
There were no measured read failures or retries. The minimum sampled
free disk was 80.44 GiB. Owned services, listeners, full-profile
containers and disposable MinIO/Postgres data were removed. The seed
was retained. The local archive is
`target/locality-tpch-q18/sf10-q18-cold-ranges-0bb0df4acf-io6-prefetch16g-gap64k.r86zo71u/`.
`gap64k-vs-gap0.json` retains both endpoints, settings, binary identity,
latency histograms, overlap observations and boundary checks.
`gap64k-vs-gap0.md` has the full comparison table. Raw startup, periodic
and whole-query samples are retained beside the summaries.
