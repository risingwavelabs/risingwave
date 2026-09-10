# Hummock Partition-Aware L0 Compaction Contract

This document records the experimental compaction contract for a Hummock compaction group that
contains exactly one state table and whose L0 SST references conform to a fixed vnode partition
layout. It also records the observability required to evaluate the policy. It is not the contract
for legacy or multi-table compaction groups.

## Terminology

- A **vnode partition** is a static key range derived from the table vnode count and the compaction
  group's `split_weight_by_vnode`.
- **Partition depth** is the number of non-empty non-overlapping L0 sub-levels containing at least
  one runnable SST reference in that vnode partition. It is a scheduling pressure proxy.
- **Maximum overlap depth** is the maximum number of runnable L0 SST key ranges covering the same
  key in that partition. It is a closer approximation of point-read amplification.
- An **SST reference** is a logical `SstableInfo` entry. Multiple references can share an object.
  Reference count, distinct object count, and range-local `sst_size` must be observed separately.

Partition depth is deliberately not called read amplification. For example, two sub-levels with
disjoint key ranges contribute a partition depth of two but a maximum overlap depth of one.

## Activation and fallback

The partition-aware policy is active only when all of the following hold:

1. The compaction group contains exactly one table.
2. The table vnode count and `split_weight_by_vnode` define more than one valid static partition.
3. Every L0 SST reference belongs to exactly one static partition.

If the fixed partition view cannot be built, L0 selection falls back to the legacy global L0
strategy. Multi-table compaction groups always use the legacy path.

## Scheduling contract

The current policy separates global L0 admission from local task construction:

1. Raw L0 pressure is the maximum partition-depth score. At least one partition must exceed the
   configured depth threshold.
2. The L0 priority is adjusted by effective Base-level fill, using the same shape as Pebble:
   `raw_l0_score / max(0.01, effective_base_fill)`. Effective fill accounts for pending incoming
   and outgoing compactions.
3. Ordinary ToBase candidates exist only for partitions above the depth threshold, and the picker
   must select at least the configured number of L0 levels. They are tried by adjusted global
   priority and then by decreasing partition pressure.
4. A shallow partition may be considered only for a trivial move. If that move is not possible,
   the attempt must not fall through to an ordinary ToBase rewrite.
5. Intra candidates also exist only for partitions above the depth threshold. At the same global
   L0 pressure, deep ToBase candidates are tried first, then shallow trivial moves, and finally
   deep Intra candidates. Each class is ordered by decreasing partition pressure.
6. A failed picker attempt means only that the current heuristic did not produce an acceptable
   task. It does not prove that no legal ToBase task exists.

`sub_level_max_compaction_bytes` is not a minimum-byte admission rule for the partition-aware
ToBase path. The existing picker still applies its L0 byte, file, and level limits. The partition
path does not apply the legacy validator, so selected L0 input plus its Base overlap has no uniform
hard total-size check. Admission uses partition depth strictly greater than the configured threshold;
the selected task must contain at least that many L0 levels.

## Task construction and conflict contract

Partition ToBase reuses the unchanged `NonOverlapSubLevelPicker` on the partition-local
non-overlapping prefix. It chooses one seed sub-level using the existing heuristic, constructs its
range closures, and checks their depth and Base pending inputs. It does not enumerate every SST or
every intermediate closure. Maximum point-overlap depth is telemetry only.

Ordinary partition ToBase attempts a depth-qualified normal task before falling back to a Base
trivial move. A normal result with one non-overlapping source level and no Base inputs retains the
existing metadata-only move behavior. Legacy remains move-first; shallow partition candidates
remain move-only. A returned normal task that fails the outer output-conflict check does not cause
another search or a move fallback within that partition.

The partition path may proceed past a fully pending oldest sub-level when a disjoint runnable stack
exists. Pending SSTs remain in the view and must still pass the existing closure checks. Legacy
retains its oldest-sub-level early return.

The selector applies the normal in-progress output-range conflict check to the returned task. A
conflict advances to the next picker candidate, not another seed in the same partition. Consequently,
both seed-layer selection and final output conflicts may hide otherwise legal tasks; the policy does
not promise exhaustive search.

### Optional free growth

Correctness closure pulls in older overlapping L0 data so newer versions do not reach Base first.
Free growth instead unions already-closed candidates to amortize one task and its Base rewrite.
Only ordinary partition ToBase uses this optional phase:

1. Accept the initial depth-qualified seed and its non-pending Base inputs. If its output already
   conflicts, return it to the normal selector check without attempting growth.
2. When Base input is non-empty, consume the remaining plans from the same NonOverlap picker call.
   A donor may be shallower than the initial depth threshold. No second picker pass, exhaustive
   seed enumeration, or cross-partition search is performed.
3. Deduplicate L0 SST IDs and reject a donor that adds none. Rebuild the union in source-level order,
   preserving each independent closure; do not validate the union as a single gap-free L0 hull.
4. Require the exact initial Base SST ID set to stay unchanged. The union must satisfy existing L0
   byte/file/level limits and the combined L0-plus-Base `max_compaction_bytes` limit. These are
   range-local `sst_size` limits, not whole-object download limits.
5. Check the complete grown output against in-progress outputs before replacing the accepted task.
   A failed donor leaves the previously accepted task intact and does not prevent later donors.

The donor loop is bounded by the already-generated plans, not an arbitrary new candidate budget.
It adds no closure construction; rebuilding a union scans the partition view for each donor.
"Free" means no additional Base SSTs, not zero write cost: added L0 input is still rewritten, and
a donor that could later move trivially need not improve write amplification.

### Relationship to Pebble and retained evidence

The source comparison is pinned to official Pebble commit
`13596f1e1cea9196defa14dec5c2ac9d90120010` (master inspected on 2026-09-10):

- [Base closure construction](https://github.com/cockroachdb/pebble/blob/13596f1e1cea9196defa14dec5c2ac9d90120010/internal/manifest/l0_sublevels.go#L1502-L1591)
  preserves version order. Optional rectangle filling is explicitly separate.
- [L0 free growth](https://github.com/cockroachdb/pebble/blob/13596f1e1cea9196defa14dec5c2ac9d90120010/compaction_picker.go#L500-L565)
  uses the neighboring unselected Base boundaries, skips empty Base inputs, and rolls back expansion
  when the combined size is too large. RW uses existing closed donors instead of porting the
  interval organizer/rectangle algorithm.
- [Size and grandparent limits](https://github.com/cockroachdb/pebble/blob/13596f1e1cea9196defa14dec5c2ac9d90120010/compaction.go#L48-L76)
  serve different purposes: expanded input size is bounded by target-file-size and disk capacity;
  grandparent overlap primarily limits individual output files, and also affects trivial-move
  eligibility. RW does not import those constants or introduce a new grandparent policy here.

RW already batches immutable memtables in upload tasks; this proposal is not based on a claim that
RW never batches flush input. Fixed vnode partitioning and upload batching do not guarantee that a
chosen L0 closure amortizes the Base SST it rewrites.

The retained 8 GiB runs do not prove a runtime growth benefit: bf57's score-plus-growth run wrote
46.48 GiB and did not isolate the two changes; depth-seed and independent-picker runs accepted
0/3 and 0/90 growth candidates respectively. Their old `merge-limit` counter mixed duplicate/subset
donors with actual limits. That evidence rejects a claim of measured benefit, but does not show
whether useful distinct donors were absent or refused. The small optional mechanism remains an
experiment, not a demonstrated fix for the workload's write amplification.

## Read and write cost model

No single counter is sufficient:

| Signal | Primary meaning |
| --- | --- |
| Maximum per-key overlap depth | Point-read probe pressure in L0 |
| Non-empty partition depth | Coarse scheduling pressure and retained sub-levels |
| Logical SST reference count | Version metadata, picker work, and range-iterator fragmentation |
| Distinct object count | Object metadata and cache identity pressure |
| Selected L0 `sst_size` | Range-local work amortized by a ToBase task |
| Target `sst_size` / selected L0 `sst_size` | Range-local Base rewrite cost paid for that L0 work |
| Referenced `file_size` sum | Existing whole-object task-size accounting; may count one split object more than once |
| Growth-added `sst_size` | Extra L0 work included without expanding the Base SST set |

Small logical references do not by themselves prove that physical output SSTs are too small.
Conversely, low overlap depth does not prove that reference count, object count, or batching cost is
irrelevant.

The current policy has no independent minimum L0 bytes or file-count admission rule. Depth is the
only batching threshold until task-level evidence can distinguish fixed task overhead from Base
rewrite cost. A total-task byte minimum would not make that distinction: a large Base overlap can
make an expensive task pass even when its L0 input is small.

## Observability contract

Partition index is emitted only in structured logs; it must not be a Prometheus label. The metrics
use bounded labels (`group`, `picker`, `outcome`, and `kind`):

- `storage_partition_l0_compaction_total`: candidate snapshots plus picker-empty, range-conflict,
  and selected outcomes. Candidate snapshots are emitted once per ordinary-ToBase or
  trivial-move-only partition on each scheduling pass; they are observation-weighted rather than
  independent version snapshots.
- `storage_partition_l0_compaction_bytes`: partition total/runnable bytes, Base current/incoming/
  outgoing/effective/target bytes, selected L0 bytes, target bytes, ordinary-ToBase initial L0
  bytes and growth-added L0 bytes,
  and the corresponding referenced-object `file_size` sums where applicable. `u64::MAX` dynamic
  Base target sentinels are excluded.
- `storage_partition_l0_compaction_count`: coarse and maximum-overlap depths, logical reference and
  distinct object counts, selected task shape, and ordinary-ToBase donor outcomes.
- `storage_partition_l0_compaction_score`: raw global, adjusted global, and partition scores,
  represented as score times 100.

Every selected partition task emits an info-level structured log under
`risingwave_meta::hummock::partition_l0`. Failed attempts are debug-level logs and aggregate into
the outcome counter. Selected-task histograms exclude failed attempts, trivial moves have their own
picker label. Growth outcome kinds are `growth-accepted`, `growth-no-new-sst`,
`growth-base-set-change`, `growth-bytes`, `growth-files`, `growth-levels`, and
`growth-output-conflict`. Each donor records its first decisive outcome; summing their histogram
sums gives donor attempts. The rejection check order is no-new-SST, files, levels, bytes, Base set,
then output conflict. Zero attempts mean growth was not attempted (for example, no remaining donor
or empty Base input), not that an expansion failed a limit.
No exhaustive-search metrics are emitted.

## Non-goals and open questions

The current policy does not claim:

- equivalence with Pebble's interval organizer or file-pressure score;
- that partition depth equals actual read amplification;
- that partition depth alone guarantees optimal write amplification;
- that the existing seed heuristic finds every legal partition task;
- that the Base pressure adjustment has been isolated as the cause of an observed regression.

These questions must be answered from selected-task observations and controlled A/B evidence, not
from aggregate task count alone.
