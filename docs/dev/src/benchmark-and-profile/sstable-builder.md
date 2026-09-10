# SST builder microbenchmarks

## Retained optimizations and measured results

The patch swaps the encoded key buffer with the previous-key buffer after each SST entry and
uses `sort_unstable` before deduplicating XOR filter hashes. It preserves the encoded bytes and
block/SST boundary decisions. A block-key suffix update and direct value encoding were tested
and rejected because they regressed other cases.

| Retained change | Workload | Observed time reduction |
| --- | --- | ---: |
| Key-buffer swap | 4096 entries, 8 B values, 32/256 B table keys | 5%-8% in the final run |
| XOR unstable sort | 1024 input hashes, four copies per key, XOR8/XOR16 | 10%-11% |

These are local ARM measurements with visible run-to-run variation. Larger values and filters
mostly had overlapping confidence intervals; no universal or end-to-end speedup is claimed.
See the experiment record below for all cases, repeat runs, binary identities and limitations.

## Running the benchmarks

`bench_sstable_builder` measures the production block and SST builders without a running cluster
or an object store service. Run from the repository root:

```shell
cargo bench -p risingwave_storage --features test --bench bench_sstable_builder -- --save-baseline before
# Apply the change, keeping the benchmark inputs, compiler and profile unchanged.
cargo bench -p risingwave_storage --features test --bench bench_sstable_builder -- --baseline before
```

The default is 30 samples, a one-second warmup and a three-second measurement window per case.
Criterion reports time per iteration and keys per second. An iteration builds 256 block entries,
4096 SST entries, or one filter of the size indicated in its benchmark ID.

| Group | Timed work | Input and limitations |
| --- | --- | --- |
| `block_builder` | `add`, `build`, `clear`, reusing the builder's buffers | Pre-encoded 32/256-byte table keys with short/long shared prefixes, 32-byte raw values, restart interval 16, no compression. |
| `sstable_builder` | Construct a builder and `InMemWriter`, add every KV, finish the complete SST, drop the output | 32/256-byte table keys and 8/256/4096-byte values; no filter or cache. Includes output buffer allocation and the writer's block copies. LZ4/Zstd cases use a fixed repeating payload and do not represent every compression ratio. |
| `xor8_finish`, `xor16_finish` | Sort, deduplicate, build and serialize the filter | 1024/262144 hashes; unique keys or four adjacent copies. Input hashing and input buffer allocation occur in untimed setup. Includes filter construction, not just sorting. |

Dataset generation is outside the timed region. The cases call the real production APIs; they do
not reproduce the algorithm in a benchmark-only implementation. Use one change at a time for
attribution, then measure the combined patch. Re-run the original and changed binaries if an
effect is near noise or depends on the order of execution. These are CPU microbenchmarks, not
claims about end-to-end compaction throughput or production x86 performance on an ARM host.

## Encoding compatibility

```shell
cargo test --release -p risingwave_storage --features test --test sstable_builder_compatibility
```

The fixed fingerprints were captured from the writer before the optimizations. They cover block
length types and restarts, builder reuse, PUT/DELETE value lengths, all three compression modes,
plain/blocked XOR8/XOR16, shortened metadata keys, vnode ranges, table switches, raw block
transfer and its small-block decode/rebuild fallback. SST fixtures also include `SstableInfo`
and the per-entry block-size/capacity decisions; wall-clock creation time is excluded.

Setting `RW_BUILDER_ORACLE_DIR` exports the complete fixture bytes. Exporting from the original
and changed binaries to different directories enables direct byte comparisons as well as the
fixed fingerprint checks. Do not update expectations on the optimized writer to make a failure
pass: first explain any difference from the original encoding. Performance runs do not replace
these compatibility checks.

## Local experiment record

<details>
<summary>2026-09-10: measurements, rejected candidates and validation</summary>


- Original product source: `7e5b5f5f085e43d1c7f648d8cd9078aaa830cd7f`.
- Host: Apple Silicon T6041, arm64, 12 CPUs, 48 GiB RAM; Darwin 25.5.0.
- Compiler: `nightly-2026-06-21`, Rust 1.98.0 nightly; default optimized bench/release
  profile with debug information. No cluster or external object store was started.
- Configuration and workload: the 18 cases above, with the default 30 samples and 1s/3s
  warmup/measurement. Criterion extended the large-filter measurement windows to gather all
  samples. The corrected original run took 88.26s.
- Corrected original benchmark binary SHA-256:
  `f33ccb23b2c72d462f84855bd54974ea3a5f308bb86204ea281bcce0e8e166ab`.
- Raw evidence and standalone round summaries: `/tmp/rw-builder-bench-20260910/`.
  `criterion-baseline-corrected/` contains the accepted original measurements;
  preliminary measurements using invalid pure-epoch inputs are superseded and excluded.
- The original writer passed all three compatibility tests with 36 exported fixtures.
  Key shortening is tested on single-table SSTs; the original shortening helper rejects
  cross-table inputs, so the separate table-switch case leaves shortening disabled.

The block-last-key suffix update was rejected and reverted. Although it preserved all encoding
fixtures, a reverse-order comparison of the saved binaries (50 samples, 1s warmup, 5s measurement,
no compilation during timing) reproduced regressions. Time below is the mean per 256 entries:

| Table key / prefix | Original | Suffix update | Time change |
| --- | ---: | ---: | ---: |
| 32 B / short | 8.219 us | 9.020 us | +9.75% |
| 32 B / long | 8.217 us | 7.341 us | -10.67% |
| 256 B / short | 10.166 us | 10.994 us | +8.15% |
| 256 B / long | 18.413 us | 18.519 us | +0.58%, overlapping 95% CIs |

The other three cases had non-overlapping 95% mean confidence intervals. Evidence:
`block-repeat-summary.txt`, `block-repeat-deltas.tsv`, and the `criterion-*-block-repeat/`
directories. Both runs completed and left no test/build process active. The first single-pass
comparison was less stable, which is why the saved binaries were measured again in reverse order.

The key-buffer swap alone passed all three frozen compatibility tests. Its six SST cases used
the same 30-sample configuration and took 29.93s. Mean time per 4096 entries:

| Table key / value / compression | Original | Key swap | Time change |
| --- | ---: | ---: | ---: |
| 32 B / 8 B / None | 346.082 us | 328.827 us | -4.99% |
| 256 B / 8 B / None | 548.773 us | 539.768 us | -1.64% |
| 32 B / 256 B / None | 412.690 us | 406.072 us | -1.60% |
| 32 B / 4096 B / None | 1613.396 us | 1635.212 us | +1.35% |
| 32 B / 256 B / LZ4 | 1288.126 us | 1277.459 us | -0.83% |
| 32 B / 256 B / Zstd | 782.820 us | 777.486 us | -0.68% |

Only the 32 B key with 8 B/256 B uncompressed values had non-overlapping 95% mean confidence
intervals in this round. Evidence: `stage2-ready-summary.txt`, `stage2-deltas.tsv`,
`stage2-product-diff.patch`, `stage2-binary.sha256`, and `criterion-stage2/`. No build or test
process remained active at the end of the round.

Direct value encoding was then added on top of the key swap. **It was ultimately rejected and
reverted due to a reproducible 256 B value regression.** It passed all three frozen tests.
The first six-case run (30 samples, 1s/3s, 28.91s total) showed 2.36%/1.79%/6.01%/12.92%/5.61%
slowdowns versus the original for the small-value cases and a 5.28% improvement for 4096 B
values. Those initial results were not accepted as stable: the saved direct-value binary was
repeated before the saved key-swap-only binary, with 50 samples and 1s/5s windows:

| Table key / value / compression | Key swap only | Plus direct value | Time change |
| --- | ---: | ---: | ---: |
| 32 B / 8 B / None | 347.807 us | 334.563 us | -3.81% |
| 256 B / 8 B / None | 571.447 us | 547.564 us | -4.18%, overlapping CIs |
| 32 B / 256 B / None | 435.592 us | 415.065 us | -4.71% |
| 32 B / 4096 B / None | 1686.625 us | 1487.607 us | -11.80% |
| 32 B / 256 B / LZ4 | 1354.392 us | 1376.162 us | +1.61%, overlapping CIs |
| 32 B / 256 B / Zstd | 850.422 us | 797.545 us | -6.22% |

The initial small-value regressions did not reproduce. The other four repeat cases had
non-overlapping 95% mean confidence intervals, but the variation between rounds limits precise
attribution of small differences. Evidence: `stage3-ready-summary.txt`, `stage3-deltas.tsv`,
`stage3-vs-stage2-repeat-deltas.tsv`, `stage3-product-diff.patch`, `stage3-binary.sha256`, and
`criterion-stage3/`, `criterion-stage3-repeat/`, `criterion-stage2-repeat/`. All runs finished
and left no test/build process active.

The three-change candidate (key swap, direct value encoding, XOR sorting) was measured before
the original binary across all 18 cases, with 50 samples and 1s/5s windows. Its binary SHA-256
was `1dd356c9563241e9304d851870038721015784b66def58ac4fb853e1ce1e23ae`. All three release
compatibility tests passed; all 36 exported fixtures matched the original byte for byte.
Evidence: `final-bench-summary.txt`, `final-pair-deltas.tsv`, `criterion-final/`,
`criterion-final-before/`, and `oracle-compare.txt`. These are records of a superseded candidate,
not the final retained patch.

That round showed a 15.90% improvement for 4096 B values, but apparent block-control regressions
of 2.59%-7.11% and a 3.71% increase for uncompressed 256 B values. Five controls were therefore
repeated separately in original/candidate/candidate/original order, 30 samples and 1s/3s each.
The block controls varied substantially within the same binary. The 256 B SST case, however,
showed a consistent regression in both candidate runs:

| 4096-entry SST, 32 B key / 256 B value / no compression | Mean | 95% mean CI |
| --- | ---: | ---: |
| Original A | 430.853 us | 426.784-435.650 us |
| Direct-value candidate A | 478.214 us | 457.552-501.588 us |
| Direct-value candidate B | 463.881 us | 449.764-477.513 us |
| Original B | 431.938 us | 427.754-436.196 us |

The candidate was 7.40%-10.99% slower in the corresponding A/B comparisons. The large-value
benefit did not justify this tradeoff for a low-risk optimization. Direct value encoding was
removed, restoring `block.rs` exactly to the original source and retaining the original
intermediate value buffer. Evidence: `control-repeat-summary.txt`, `control-repeat-rounds.tsv`,
and `criterion-control-repeat/`. The initial final-validation attempt was interrupted before
tests executed; it is superseded by validation of the two-change patch.

XOR `finish` is independent of the key/value builder changes. Its measurements in the
18-case round remain applicable to the retained, unchanged `sort_unstable` change:

| Filter / input count / copies per key | Original | Unstable sort | Time change |
| --- | ---: | ---: | ---: |
| XOR8 / 1024 / 1 | 15.055 us | 15.083 us | +0.19% |
| XOR8 / 1024 / 4 | 8.339 us | 7.389 us | -11.40% |
| XOR8 / 262144 / 1 | 9360.207 us | 8664.749 us | -7.43% |
| XOR8 / 262144 / 4 | 3876.780 us | 3793.227 us | -2.16% |
| XOR16 / 1024 / 1 | 17.962 us | 17.422 us | -3.01% |
| XOR16 / 1024 / 4 | 9.545 us | 8.548 us | -10.44% |
| XOR16 / 262144 / 1 | 9949.486 us | 9459.686 us | -4.92% |
| XOR16 / 262144 / 4 | 4078.795 us | 3971.595 us | -2.63% |

Only the two 1024-input, four-copies cases had non-overlapping 95% mean CIs. Other differences
should be treated as uncertain. Sorting equal `u64` hashes does not depend on stability:
the sorted and deduplicated input to the XOR constructor is unchanged. This also removes the
stable sort's temporary allocation; these runs measure elapsed CPU-path time, not allocation
counts or peak RSS.

The next candidate retained key-buffer swapping and XOR unstable sorting. Its binary SHA-256
was `74dfe3fde78eca68cfe0025d1397e441e7902da0336109e034629baebe722f79`. The six SST cases were
measured candidate first, original second, with 50 samples and 1s/5s windows:

| Table key / value / compression | Original | Key swap + XOR sorting | Time change |
| --- | ---: | ---: | ---: |
| 32 B / 8 B / None | 373.321 us | 345.236 us | -7.52% |
| 256 B / 8 B / None | 568.185 us | 538.880 us | -5.16% |
| 32 B / 256 B / None | 463.653 us | 495.778 us | +6.93%, overlapping CIs |
| 32 B / 4096 B / None | 1661.180 us | 1654.946 us | -0.38%, overlapping CIs |
| 32 B / 256 B / LZ4 | 1315.571 us | 1389.995 us | +5.66% |
| 32 B / 256 B / Zstd | 843.627 us | 811.846 us | -3.77%, overlapping CIs |

All three release compatibility tests passed and all 36 exported fixtures were byte-identical
to the original. Evidence: `final2-bench-summary.txt`, `final2-deltas.tsv`,
`final2-product-diff.patch`, `final2-binary.sha256`, and `oracle-final2-compare.txt`.
The LZ4 case was repeated alone in original/candidate/candidate/original order, using saved
binaries, 30 samples and 1s/3s windows. Original means were 1390.104/1353.331 us; candidate means
were 1296.149/1312.601 us. Both candidate runs were faster than both originals, so the initial
LZ4 regression did not reproduce. Evidence: `key-swap-lz4-summary.txt`,
`key-swap-lz4-rounds.tsv`, and `criterion-key-swap-lz4/`.

The final patch retains key-buffer swapping and XOR unstable sorting. The small uncompressed
values showed improvement in both the isolated key-swap and final two-change runs. The exact
size of small gains remains uncertain on this shared host, especially for 256 B values; the
measurements do not establish a universal speedup. Block suffix updates and direct value
encoding remain rejected. No encoder, decoder, block format, restart rule, compression setting,
checksum, or boundary calculation changes in the retained patch.

Final validation:

| Check | Result |
| --- | --- |
| Existing `hummock::sstable::` library tests, release | 45 passed |
| Existing `hummock::sstable::` library tests, debug | 45 passed |
| Frozen compatibility tests, release | 3 passed; 36/36 exported files byte-identical |
| Frozen compatibility tests, debug | 3 passed, including debug assertions |
| `cargo clippy --workspace --all-targets --features risingwave_storage/test -- -D warnings` | Passed |
| Rust formatting and `git diff --check` | Passed |

Logs and the standalone `final2-validation-summary.txt` are retained in the evidence directory.
No Cargo, benchmark, or test process remains running. No cluster or external service was started;
raw evidence and saved binaries are retained locally and are not part of the source patch.

Before PR submission, the branch was fast-forwarded to main
`4e0e84ae5b2c2386cfb89d39ab1312620719f96f`. Cleanup changed field comments and documentation
organization only. On that base, debug/release SST tests (45 each), debug/release compatibility
tests (3 each), workspace clippy with `-D warnings`, formatting and diff checks passed again.
The standalone record is `pr-validation-summary.txt`; benchmark numbers above remain tied to
the explicitly recorded original base and binaries.

</details>
