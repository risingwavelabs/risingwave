# SF10 Q18 read scheduler comparison

Three runs of the simplified branch on macOS ARM64 with `risedev d full` and local MinIO, using three archived runs per comparison cohort. The old all-on cohort keeps the same two-worker topology and read limits and uses the former 100 µs timer. Clean main is the historical Range 4 GiB endpoint.

## Method

- Each run used an independent cold APFS clone of the same Hummock v1346 seed. The SQL, initial SST fingerprint, EXPLAIN and 100-row oracle matched across all nine runs.
- The workload scanned 136,472,104 source rows. One CN ran 53 Q18 actors and six LocalityProviders with fixed4 session parallelism and eight streaming workers. The two optimized builds advertised 12 slots and used two dedicated I/O workers. Clean main advertised four slots.
- The simplified binary was built from `b3a789fea2e5785b34d7de222c0bf7a03c586bee` with Foyer `02d6b5ebc7516121ab0ad84ad8780623cfa1c8c5`. All three runs reused opt3 executable SHA-256 `acdebd10ee08d3da04ffe63472249c79b2b612cc86467bb77d87dafbc0cf9797`. The build used `split-debuginfo=unpacked` and the repository test license with debug assertions scoped to the license crate.
- Both optimized cohorts enabled source Snapshot Backfill, Locality and HashJoin prefix Blockwise, point MultiGet, metadata reuse, I/O coalescing, in-flight reuse and the dedicated I/O runtime. G128/P32, N16/B16, zero gap and a 1 MiB merge ceiling were fixed. MultiGet used concurrency 10, 16 retained entries and a 1 MiB retention limit. Changelog Blockwise, Locality sort buffering and tracing were off. The six-worker GET-only runs used a separate configuration.
- The measured interval is foreground CREATE MATERIALIZED VIEW. Source cutoff is the first full scrape with the expected source counts. Operator transitions use LocalityProvider start logs, with counters from the nearest available scrape. I/O metrics cover the CN and include concurrent operators. GET means a successful CN physical object-store read.
- Rates and percentiles below are per-run observations. Median columns use medians of those observations. Percentiles are histogram bucket upper bounds. Depths come from periodic scrapes and show observed occupancy. Reserved-to-first-backend-poll and iterator consumer wait are reported separately in the data.
- Each run completed with zero read errors and retries. All owned test services, listeners and clones were cleaned up.

[Q18 SQL](q18.sql) · [Simplified settings](q18-read-path.toml) · [Per-run metrics, buckets and startup samples](q18-sf10-scheduler-simplify.json)


## Cohort medians

| Metric | Clean main | Previous all-on | Simplified all-on | Simplified vs previous |
| --- | ---: | ---: | ---: | ---: |
| Build MV, s | 567.94 | 628.77 | 544.87 | -13.3% |
| Source checkpoint, s | 75.64 | 81.05 | 81.12 | +0.1% |
| Physical GETs | 125,961 | 122,290 | 141,460 | +15.7% |
| GETs ≤64 KiB | 56,117 | 58,026 | 71,157 | +22.6% |
| Through source GETs | 39,577 | 40,492 | 46,000 | +13.6% |
| After source GETs | 86,384 | 81,570 | 95,418 | +17.0% |
| Read GiB | 37.356 | 36.161 | 36.109 | -0.1% |
| GET mean, ms | 9.55 | 2.17 | 1.94 | -10.8% |
| GET p99, ms | 256 | 16 | 16 | +0.0% |
| GET p99.9, ms | 512 | 64 | 32 | -50.0% |
| Scheduled logical read p99.9, ms | — | 568.2 | 378.8 | -33.3% |
| Startup admission >327.68 ms, % | — | 15.61 | 4.48 | -71.3% |
| Blockwise consumer wait p99, ms | — | 2.048 | 2.048 | +0.0% |
| Peak CN RSS, MiB | 6,165 | 5,835 | 5,763 | -1.2% |

## Individual runs

| Run | Build MV s | Source s | GETs | GiB | GET ≤64 KiB | GET mean ms | p99 ms | p99.9 ms | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Main 1 | 567.94 | 80.80 | 123,672 | 37.178 | 54,627 | 9.55 | 256.00 | 512.00 | 6,018 |
| Main 2 | 549.13 | 75.61 | 125,961 | 37.356 | 56,117 | 8.70 | 256.00 | 512.00 | 6,176 |
| Main 3 | 629.96 | 75.64 | 127,044 | 37.619 | 56,911 | 10.08 | 256.00 | 512.00 | 6,165 |
| Previous 1 | 628.77 | 81.05 | 122,290 | 36.161 | 58,297 | 2.17 | 16.00 | 64.00 | 5,835 |
| Previous 2 | 691.22 | 85.94 | 122,978 | 36.238 | 58,026 | 2.24 | 32.00 | 64.00 | 5,863 |
| Previous 3 | 554.00 | 81.04 | 121,539 | 35.717 | 57,924 | 2.13 | 16.00 | 64.00 | 5,831 |
| Simplified 1 | 521.21 | 80.91 | 143,707 | 36.178 | 71,195 | 1.94 | 16.00 | 32.00 | 5,763 |
| Simplified 2 | 625.05 | 81.12 | 139,202 | 36.109 | 69,560 | 1.92 | 16.00 | 32.00 | 5,792 |
| Simplified 3 | 544.87 | 86.07 | 141,460 | 35.545 | 71,157 | 1.95 | 16.00 | 32.00 | 5,713 |

## Physical reads before and after the source checkpoint

| Run | Scan GETs | Scan ≤64 KiB | Scan GiB | Scan GET mean ms | Scan group members | Scan single groups | Later GETs | Later ≤64 KiB | Later GiB | Later GET mean ms | Later group members | Later single groups |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Main 1 | 39,959 | 1,217 | 13.535 | 25.23 | — | — | 83,713 | 53,410 | 23.642 | 2.06 | — | — |
| Main 2 | 39,577 | 1,273 | 13.293 | 23.14 | — | — | 86,384 | 54,844 | 24.063 | 2.09 | — | — |
| Main 3 | 39,572 | 1,276 | 13.292 | 27.65 | — | — | 87,472 | 55,635 | 24.327 | 2.14 | — | — |
| Previous 1 | 40,492 | 4,653 | 12.321 | 3.60 | 13.40 | 3,360 | 81,798 | 53,644 | 23.840 | 1.47 | 4.85 | 52,879 |
| Previous 2 | 41,408 | 4,992 | 12.542 | 3.79 | 13.26 | 3,591 | 81,570 | 53,034 | 23.695 | 1.46 | 4.82 | 52,471 |
| Previous 3 | 40,269 | 4,648 | 12.264 | 3.50 | 13.38 | 3,394 | 81,270 | 53,276 | 23.454 | 1.45 | 4.86 | 52,489 |
| Simplified 1 | 46,000 | 9,038 | 12.353 | 3.05 | 11.81 | 6,738 | 97,707 | 62,157 | 23.824 | 1.41 | 4.03 | 61,525 |
| Simplified 2 | 45,710 | 8,506 | 12.458 | 3.16 | 11.91 | 6,376 | 93,492 | 61,054 | 23.651 | 1.32 | 4.18 | 60,267 |
| Simplified 3 | 46,042 | 8,865 | 12.410 | 3.22 | 11.73 | 6,735 | 95,418 | 62,292 | 23.135 | 1.34 | 4.09 | 61,445 |

## Scheduler behavior

| Run | Logical reads | Groups | Members/group | Startup admit >327.68 ms | Startup admit p99 ms | Later admit p99 ms | Logical p99.9 ms | Dispatch p99 ms | Consumer p99 ms | Peak G/P started | Peak logical waiters |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Previous 1 | 918,752 | 119,183 | 7.71 | 12.74% | 655.36 | 5.12 | 568.2 | 40.96 | 2.048 | 128/32 | 21,602 |
| Previous 2 | 922,657 | 120,086 | 7.68 | 15.61% | 655.36 | 5.12 | 568.2 | 40.96 | 2.048 | 127/32 | 22,078 |
| Previous 3 | 914,746 | 118,603 | 7.71 | 18.97% | 655.36 | 5.12 | 568.2 | 40.96 | 2.048 | 128/32 | 27,825 |
| Simplified 1 | 920,458 | 140,879 | 6.53 | 4.00% | 655.36 | 0.64 | 378.8 | 40.96 | 2.048 | 128/32 | 19,405 |
| Simplified 2 | 918,069 | 136,167 | 6.74 | 4.48% | 655.36 | 0.64 | 378.8 | 40.96 | 2.048 | 128/32 | 19,449 |
| Simplified 3 | 913,590 | 138,563 | 6.59 | 4.61% | 655.36 | 0.64 | 378.8 | 40.96 | 2.048 | 128/32 | 17,761 |

## Downstream critical path

| Run | 24→22 wall s | CN CPU-s | Table 38 SST candidates M | GETs | GET service s | 22→20 wall s | CN CPU-s | Table 32 SST candidates M | GETs | GET service s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Previous 1 | 187.00 | 817 | 164.44 | 46,974 | 43.39 | 157.02 | 698 | 94.78 | 14,516 | 24.39 |
| Previous 2 | 233.00 | 984 | 194.66 | 48,128 | 43.85 | 181.00 | 780 | 137.85 | 14,347 | 21.28 |
| Previous 3 | 152.94 | 712 | 104.22 | 46,395 | 42.81 | 138.99 | 650 | 77.86 | 14,806 | 24.42 |
| Simplified 1 | 142.87 | 707 | 120.00 | 51,466 | 47.41 | 130.00 | 581 | 59.05 | 22,646 | 36.42 |
| Simplified 2 | 215.00 | 936 | 195.00 | 49,053 | 42.94 | 138.00 | 634 | 79.94 | 22,659 | 33.66 |
| Simplified 3 | 152.00 | 701 | 119.17 | 48,907 | 43.89 | 139.10 | 628 | 77.08 | 22,524 | 33.93 |

## Interpretation

The three simplified runs issued 139,202–143,707 GETs, above the full range of 121,539–122,978 in the previous all-on cohort. The median increased by 19,170 GETs and the median number of GETs at most 64 KiB increased by 13,131, while median bytes stayed within 0.2%. The scheduled logical read median stayed near 919,000. Average members per physical group fell from 7.71 to 6.59. The source and post-source group means both fell, from 13.38 to 11.81 and from 4.85 to 4.09 respectively. Zero-gap configuration recorded zero overread bytes in all six optimized runs. The higher GET count follows from smaller physical groups under comparable logical read volumes.

The simplified plug closes after a yield from the first actual outer read or on its member or byte limit. Late reads then enter normal admission. The run-level histograms establish the reduction in group size. Assigning the lost merges to individual plug closures, late callbacks or cache gaps would require window-correlated evidence. Seal counters changed meaning across builds and should be read within each cohort.

Physical GET p99.9 fell from the 64 ms bucket to 32 ms in every simplified run. The startup admission share above 327.68 ms fell from 12.74–18.97% to 4.00–4.61%. Startup admission p99 remained in the 655.36 ms bucket. After startup, admission p99 fell from 5.12 to 0.64 ms in every run. Whole-run dispatch p99 and Blockwise consumer wait p99 landed in the same buckets across both optimized cohorts. Both configurations reached their G and P limits in the samples. Median cumulative physical GET service rose from 265.9 to 276.2 seconds across all workers despite the lower per-read average.

The simplified build-MV times ranged from 521.21 to 625.05 s and overlapped both historical ranges. Source scan timing was close to the previous all-on cohort. Simplified runs 1 and 2 differ by 104 s, including 72 s in the 24→22 interval. Table 38 candidate checks there grew from 120M to 195M and CN CPU time grew from 707 to 936 CPU-s, while cumulative GET service in that interval fell from 47.4 to 42.9 s. Downstream SST candidate work explains a substantial part of the wall-time variability. The historical ordering and evolving intermediate SST work limit attribution of the total wall-time delta to scheduler policy.

The JSON records a one-sample discrepancy between the physical latency and successful-size histograms on either side of one phase boundary in simplified run 3. The whole-query counts reconcile exactly in all nine runs.
