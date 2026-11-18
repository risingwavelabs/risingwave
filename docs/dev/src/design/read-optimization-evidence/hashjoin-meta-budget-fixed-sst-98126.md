# Fixed-SST HashJoin metadata retention replay

A fixed set of overlapping SSTs can benefit from retaining all the metadata handles a probe chunk uses. The default 1 MiB budget fits this replay, cutting Foyer meta lookups by 99.54% and median replay wall time by 12.04% versus an intentionally constrained positive budget. The constrained batch saves 15.33% of lookups versus reuse off, with nearly identical replay time.

## Controls and input

The optimized binary is built from `98126f898412f675ebdd406c35cd273f4972dd2b` plus the [recorded patch](hashjoin-meta-budget-fixed-sst-98126.patch.gz) containing the existing deferred SST path construction and the benchmark fixture option. Its SHA-256 is `4a0a63aaa6ff7228af4be50499f24cee84621a75bf57b446f2597920c9051c2f`. All arms use the same release binary (opt-level 3, debug assertions off, unpacked debug info), real HashJoin/StateTable/Hummock/Foyer and local MinIO. A shared host lease serializes the run. The background worker runtime has four threads; one join actor owns the ordered input. The scheduler stays on, tracing and Foyer file cache stay off, and the block/meta caches stay at 128/64 MiB.

The constructed fixture has 32,768 distinct keys, 8 rows per key, 256 probes per chunk and 13 overlapping L0 SSTs created in separate commits. Each key’s rows go to a single SST, with adjacent keys interleaved across the files. This preserves real overlap while reproducing the many negative filters in the Q18 phase. Actual SST ranges confirm that all 128 chunks reference the same 13 SSTs. The measured 425,828 candidate checks (12.995 per probe) match the range-derived count. Of these, 393,048 (92.30%) are filter negatives. Every arm verifies the pinned-version and SST fingerprints, warms all data blocks and checks all 262,144 output rows. No physical GET, meta miss, data miss, or object read occurs in any timed sample.

The SSTs total 33.63 MiB with 546 blocks. The 13 Foyer metadata entries weigh 128,618 bytes together (about 125.6 KiB). The constrained budget is 19,792 bytes, enough for any two handles and too small for any three. The default 1 MiB budget fits all 13. The limit on regular entries is 16 throughout. Both the narrow budget and the overlapping file geometry are experimental controls. The Q18 intermediate snapshot and its actual per-chunk handle weights are unavailable.

## Five interleaved samples per arm

| Arm | Meta budget KiB | Foyer meta lookups | Batch hits | Rejects | Median replay ms (min–max) |
| --- | ---: | ---: | ---: | ---: | ---: |
| off | - | 425,828 | 0 | 0 | 419.9 (411.9–421.5) |
| tight | 19.33 | 360,566 | 65,262 | 360,310 | 418.4 (417.1–421.7) |
| fit | 1024.00 | 1,664 | 424,164 | 0 | 368.0 (364.0–369.8) |

The median left-side HashJoin matching timer is 371.0 ms off, 372.4 ms constrained and 321.5 ms at 1 MiB. It includes iterator and lookup work, so it is elapsed time inside the operator. Process CPU samples have one-second resolution and cannot time these sub-second passes. Endpoint RSS medians are 244.7, 244.1 and 243.9 MiB. These endpoints cannot establish peak or concurrent production memory.

At 1 MiB the executor needs one Foyer lookup per SST per chunk, for 13 × 128 = 1,664 lookups. The unchanged candidate and filter counts show that retaining handles alone accounts for the lookup-count reduction. With two handles retained, the other eleven recur throughout the same chunk. Probe reordering cannot change that working set. The experiment supports measuring real Q18 chunk cardinality and Foyer handle weight before changing its budget. It does not establish a production budget value or require changing LSM pruning, prefetching, or scheduling.

The [sample matrix and candidates](hashjoin-meta-budget-fixed-sst-98126.json) retain all values, versions and weights. [Selected raw metrics and configs](hashjoin-meta-budget-fixed-sst-98126-raw.json.gz) contain the original before/after lines. The dedicated MinIO process exited after the matrix. The fixture and the staged release executable remain available for local reruns.
