# Vnode-partitioned `commit_epoch` for single-table groups

## Goal

When a compaction group contains exactly one state table and `split_weight_by_vnode > 1`, commit
SSTs into **multiple L0 sub-levels**, where each sub-level is **NonOverlapping** (i.e. `can_concat`
holds within that sub-level). If SSTs overlap after splitting, they are placed into the next layer.

This improves downstream compaction parallelism and query pruning by aligning newly committed L0
SST metadata with vnode partitions.

## Scope / enablement

Enabled only when all conditions hold:

- The compaction group is **single-table** (`member_table_ids.len() == 1`).
- `CompactionConfig.split_weight_by_vnode > 1` (treated as **partition count**).
- All committed SSTs for this group are **single-table** and have finite key ranges.

Otherwise, `commit_epoch` falls back to the original behavior: split committed SSTs into multiple
**Overlapping** L0 sub-levels by total size.

## Key idea: split only at partition boundaries

Instead of intersecting an SST with every partition range, we:

1. Precompute vnode partition boundary keys (monotonic):
   - `partition_count = split_weight_by_vnode`
   - `vnode_count = table.vnode_count()`
   - Boundaries are at vnode indices where the partition changes.
   - Each boundary is encoded as a full key:
     - `FullKey(table_id, vnode_boundary, epoch=MAX)`

2. For each committed SST, we check which boundaries fall strictly inside its key range and split
   only on those boundaries. An SST that does not cross boundaries is not split.

Notes:

- This is a **logical split**: `object_id` is unchanged. We create additional `SstableInfo` entries
  with narrower `key_range` and fresh `sst_id`s for bookkeeping.
- `sst_size` is approximated by evenly distributing the original size across split pieces (bounded
  by `>= 1`). This affects only strategy/heuristics, not correctness.

Implementation: `commit_epoch/commit_epoch_vnode_partition.rs`.

## Layering: make each L0 sub-level non-overlapping

After splitting, we have a list of SST fragments (some may still overlap). We build layers with a
greedy interval partitioning algorithm:

- Sort fragments by `key_range`.
- Put each fragment into the first layer where it does not overlap the layer’s last SST; otherwise
  create a new layer.
- Each layer is then guaranteed to be `can_concat` (non-overlapping).

To preserve commit semantics when layers overlap each other, layers are ordered by `max_epoch`
ascending before emitting them as `NewL0SubLevel` deltas, so newer epochs appear on top of older
ones.

Implementation:

- `build_nonoverlapping_layers`
- `sort_layers_by_max_epoch_asc`
- `chunk_nonoverlapping_layer_by_size` (keeps delta size bounded)

## Applying the delta

We emit one `IntraLevelDelta` per sub-level so that we can carry `vnode_partition_count`.

- `vnode_partition_count = split_weight_by_vnode` for partitioned single-table groups.
- `vnode_partition_count = 0` for fallback paths.

In apply:

- If `vnode_partition_count > 0` **and** `can_concat` holds, the sub-level is marked
  **`NonOverlapping`**.
- Otherwise it stays **`Overlapping`** (backward compatible).
