# Production branch SF10 Q18 regression check

The production branch finished CREATE MV in 417.80 s, 1.9% below
the median of the three earlier optimized runs and within 1.3% of
their fastest run. Whole-query GETs, bytes, and RSS were also lower
than the earlier optimized medians. The source interval performed
more GETs while finishing sooner.

## Experimental controls

- Production branch `li0k/io-read-scheduler-production-pr` at
  `5ba832983a5643412b2e5fd6b60269441870100d` used a clean checkout.
  Its binary SHA-256 was
  `15c5c98e39cde7cd25e85a38f7e01a618e9735c075986dcbbbe9e1ea94321e3b`.
  The opt3 build used unpacked debug info and the same opt3 test-license
  crate profile as the [fixed-8 matrix](fixed8-matrix.md).
- The production arm used `risedev d full` under the shared local
  test lease, a fresh APFS clone of the same stopped v1346 seed, local
  MinIO, normal compaction, tracing off and Foyer disk cache off.
  Compute used 12 slots, fixed streaming parallelism 8, eight streaming
  workers, 12 batch workers, and six dedicated I/O workers.
- Both optimized configurations used 4 GiB of prefetch memory,
  ColdRanges N16 / 1 MiB, native MultiGet, 9 MiB meta reuse,
  G128 / P32 / B16, a 64 KiB gap, a 1 MiB coalescing limit, and
  in-flight reuse. The production config uses `storage.scan_prefetch`.
- The source SST projection, EXPLAIN with six LocalityProviders,
  placement and vnode partitions for 105 actors, source count of
  136,472,104 rows, and independent 100-row oracle all matched.
  One production measurement was compared with the three archived
  optimized arms at `98126f8984` and their clean-main medians.
- This comparison covers the integrated branch, including the merge
  from main and subsequent read-path changes. Intermediate SST
  shapes differed under normal compaction. The single production
  arm shows no end-to-end regression in this workload.

## Results

Provider numbers are fragment IDs from backfill-start events. Source
I/O covers the CN through the first scrape with the exact expected
source row count. Backend GET latency excludes scheduler waits.

| Metric | Clean main median | Prior optimized median | Prior optimized range | Production | vs. prior optimized median |
| --- | ---: | ---: | ---: | ---: | ---: |
| CREATE MV elapsed, s | 648.05 | 425.81 | 423.20–490.13 | 417.80 | -1.9% |
| Source scan, s | 83.37 | 77.33 | 76.28–83.34 | 75.29 | -2.6% |
| DDL to provider 24, s | 256.14 | 191.13 | 187.04–210.15 | 190.05 | -0.6% |
| Provider 24 to 22, s | 130.00 | 73.01 | 70.00–109.02 | 67.00 | -8.2% |
| Provider 22 to 20, s | 113.00 | 110.01 | 106.00–113.00 | 102.00 | -7.3% |
| Provider 20 to MV completion, s | 155.85 | 57.96 | 53.05–58.77 | 58.75 | +1.4% |
| CN CPU, s | 3,652 | 2,517 | 2,512–2,646 | 2,414 | -4.1% |
| CN peak RSS, GiB | 5.703 | 5.620 | 5.516–5.624 | 5.505 | -2.1% |
| Target HashJoin candidate Bloom checks | 80,331,564 | 74,978,204 | 58,824,676–100,552,432 | 82,391,372 | +9.9% |
| Target HashJoin Foyer meta queries | 80,331,716 | 293,306 | 230,102–833,552 | 322,157 | +9.8% |
| Target HashJoin meta misses | 25 | 5 | 2–7 | 5 | +0.0% |
| Target HashJoin iterator init, actor-s | 770.60 | 301.43 | 263.93–432.88 | 250.15 | -17.0% |
| Target HashJoin CN GETs | 46,087 | 46,700 | 45,373–46,752 | 46,848 | +0.3% |
| Final HashAgg Foyer meta queries | 84,135,809 | 8,985,684 | 8,880,592–9,566,077 | 9,789,380 | +8.9% |
| Whole run physical GETs | 126,200 | 120,965 | 117,506–121,745 | 120,008 | -0.8% |
| Whole run read GiB | 37.757 | 36.281 | 36.198–36.318 | 36.110 | -0.5% |
| Whole run backend GET mean, ms | 18.925 | 2.341 | 2.236–2.753 | 2.006 | -14.3% |
| Whole run backend GET p99 upper bound, ms | 512 | 16 | 16–32 | 16 | +0.0% |
| Source interval physical GETs | 39,448 | 38,694 | 38,102–39,432 | 39,780 | +2.8% |
| Source interval read GiB | 13.298 | 12.443 | 12.379–12.462 | 12.414 | -0.2% |
| Source interval backend GET mean, ms | 36.956 | 3.426 | 3.377–3.463 | 2.907 | -15.2% |
| Source interval backend GET p99 upper bound, ms | 512 | 32 | 32–32 | 16 | -50.0% |
| Post-source interval physical GETs | 86,782 | 81,533 | 79,404–83,051 | 80,228 | -1.6% |
| Post-source interval read GiB | 24.456 | 23.819 | 23.754–23.939 | 23.697 | -0.5% |
| Post-source interval backend GET mean, ms | 10.628 | 1.816 | 1.689–2.423 | 1.560 | -14.1% |
| Post-source interval backend GET p99 upper bound, ms | 256 | 16 | 16–16 | 16 | +0.0% |

## Read-path observations

- The source interval issued 39,780 GETs, up 2.8% from the prior
  optimized median. All arms formed 38,229 prefetch windows in this
  interval. The prior arm with the median source GET count recorded
  43,206 direct cold runs and 856 shared singletons. Production
  recorded 44,675 and 1,352, and recorded more cache-gap splits
  (7,802 versus 5,833). The source read volume stayed near 12.4 GiB.
  There were zero prefetch budget denials or shrinks in either run.
  The production arm placed 596 other-bound requests through the
  scheduler in the source interval. The older median arm had 610
  backend reads outside successful scheduler groups in this interval.
- During the target HashJoin, both performed 15 million probes.
  Production made 82.39 million Bloom checks, 9.9% above the prior
  optimized median. It reused 82.07 million ready meta handles,
  saw 5 meta misses and spent 250.1 actor-seconds on iterator
  initialization. The prior optimized median was 301.4 actor-seconds.
- The last aggregation/materialization interval was 58.75 s, within
  the prior range of 53.05 to 58.77 s. Table 27 made 9.79 million
  Foyer metadata lookups, 0 metadata misses, and 104.37 million Bloom
  checks. Its metadata lookup count rose alongside its candidate
  count. Both the HashJoin and aggregate work depend on the live
  intermediate SST layout.
- Source-phase scheduler admission p99 bucket upper bound was
  163.84 ms versus the prior 327.68 to 655.36 ms. Admitted-to-dispatch
  p99 bucket upper bound stayed at 40.96 ms. The first scrape after
  5 s already contained 99.89% of production's total cumulative
  admission wait. The production arm had no backend read errors or
  retries.

The run passed its checks, stopped its owned listeners and containers,
and removed its cold clone. The temporary build cache was cleaned
after archiving the binary.

[Build identity, per-arm comparison, and measured counters](production-pr-regression.json)
