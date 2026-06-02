# Hummock Partitioned SST Meta and Filter

<!-- toc -->

## Background

Current Hummock SST metadata is stored as one encoded `SstableMeta` tail:

```text
data blocks
SstableMeta { block_metas, bloom_filter, ... }
checksum | version | magic
```

`SstableStore::sstable()` reads `meta_offset..EOF`, decodes the full `SstableMeta`, and caches a full `Sstable` object. This means that accessing one small key range may still load all block metadata and the full SST filter into memory.

The v3 format should make SST metadata incrementally loadable. A new binary must keep reading v1/v2 SSTs. An old binary is not expected to read v3 SSTs.

## Goals

- Introduce a v3 SST meta format with `version = 3`.
- Load SST metadata incrementally on object storage.
- Avoid caching the full `SstableMeta` as the normal v3 representation.
- Keep data blocks in the existing data block cache.
- Support fast compaction with the ideal raw-copy unit:

```text
N raw data blocks + 1 MetaShard filter payload
```

## Non-goals

- Reconstruct a full legacy `SstableMeta` after reading the v3 index.
- Store v3 metadata pieces in the data block cache.
- Make old binaries read v3 SSTs.
- Replace data blocks or the existing data block cache.

## Current Read Path

The current read path first prunes SSTs using metadata from `SstableInfo`, then loads full SST metadata, then checks the SST filter if the read carries a filter prefix hash.

```text
ReadVersion / levels
  -> prune SSTs by SstableInfo.table_ids and SstableInfo.key_range
  -> SstableStore::sstable(sstable_info)
       read(meta_offset..EOF)
       decode full SstableMeta { block_metas, bloom_filter, ... }
       cache full Sstable
  -> optional hit_sstable_filter(...)
       Sstable::may_match_hash(user_key_range, prefix_hash)
  -> SstableIterator::seek(...)
       locate data block by block_metas
  -> read data block
```

The filter check currently happens after full SST metadata is loaded, because the filter reader and block metadata both live inside the cached `Sstable`.

## v3 Physical Layout

The proposed v3 layout is:

```text
data blocks
MetaShard 0 { block_metas for N data blocks, one filter for these N data blocks }
MetaShard 1 { block_metas for N data blocks, one filter for these N data blocks }
...
MetaPartitionIndex
checksum | version = 3 | magic
```

`SstableInfo.meta_offset` points to the start of `MetaPartitionIndex` for v3 SSTs. The fixed footer remains at the end of the object and is used to dispatch by version.

The key design choice is to co-locate block metadata and the shard filter in one physical metadata shard. RocksDB partitioned index/filter stores index partitions and filter partitions as separate block-cache entries. That works well for RocksDB because local disk/SSD IOPS is much cheaper than object-store range requests. Hummock should instead optimize the first version around object-store IOPS:

```text
RocksDB:
  read filter partition
  if positive, read index partition

Hummock v3:
  read MetaShard once
  check filter inside MetaShard
  if positive, use block_metas inside the same MetaShard
```

Decision summary:

| Topic | Decision |
|---|---|
| Filter granularity | One plain group filter covers N data blocks |
| Block metadata | Keep existing `BlockMeta` unchanged inside `MetaShard` |
| Physical metadata read unit | Co-located `MetaShard`, not separate filter and block-meta objects |
| Initial shard size | Start with 8 or 16 data blocks per shard |
| Data block cache | Keep fully separate from metadata cache |
| RocksDB borrowing | Borrow top-level partition directory idea, but do not copy separate index/filter partition I/O |

## Metadata Layers

### MetaPartitionIndex

`MetaPartitionIndex` is the small top-level directory for a v3 SST. It is not a replacement for the entire `Sstable` object. It replaces the v3 reader's need to keep a full `SstableMeta` in memory.

The index should contain global SST summary fields and shard directory entries:

```rust
pub struct MetaPartitionIndex {
    pub version: u32,
    pub block_count: u32,
    pub shard_count: u32,

    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub key_count: u64,
    pub estimated_size: u64,

    pub filter_type: PbSstableFilterType,
    pub shard_policy: ShardPolicy,

    pub shards: Vec<MetaShardDesc>,
}

pub struct MetaShardDesc {
    pub shard_idx: u32,
    pub first_block_idx: u32,
    pub block_count: u32,

    // Used to locate the shard without reading the shard body.
    pub smallest_key: Vec<u8>,

    // Physical location of the shard body in the SST object.
    pub offset: u64,
    pub len: u32,
    pub checksum: u64,
}
```

The index is used to locate `MetaShard` by key range or block index. It does not contain the filter payload and does not contain block metadata. Per-shard `largest_key`, `table_ids`, and `vnode_range` are not required in the first version:

| Field | First-version decision | Reason |
|---|---|---|
| shard `largest_key` | Do not store in desc | It can be derived from the next shard's `smallest_key` or the SST global `largest_key` |
| shard `table_ids` | Do not store in desc | Start with table-id hard split and use the key/table range already present in the index |
| shard `vnode_range` | Do not store in desc | Optional pruning summary; add only if measurements show value |

`MetaShardDesc.smallest_key` is currently shown as a redundantly stored key. An alternative is to store only `first_block_idx` and derive the shard's smallest key from the first `BlockMeta` in the shard. That saves index bytes, but it prevents key-based shard lookup before loading the shard body. This tradeoff remains open.

Shard and block boundary lookup should follow the existing Hummock block lookup semantics: decode the boundary key as `FullKey` and compare it with the seek `FullKey` using the Hummock full-key ordering. This keeps v3 block selection consistent with the legacy `SstableIterator::calculate_block_idx_by_key` path. The point-get filter-pruning path is the exception: multiple versions of the same `UserKey` can span block or shard boundaries, so a shard-level filter negative is safe only after checking the shard located by `FullKey` plus adjacent candidate shards that may contain the same `UserKey`.

### MetaShard

`MetaShard` is the physical metadata read unit. One `MetaShard` contains the unchanged `BlockMeta` list for a small group of consecutive data blocks and one group filter covering all keys in those data blocks.

```rust
pub struct MetaShard {
    pub shard_idx: u32,
    pub first_block_idx: u32,
    pub block_metas: Vec<BlockMeta>,

    // One filter for all keys in these block_metas.
    pub filter: MetaShardFilter,
}

pub struct MetaShardFilter {
    pub filter_type: PbSstableFilterType,
    pub payload: Vec<u8>,
}
```

`BlockMeta` itself should not need a format change for the first version.

The filter is a group filter:

```text
blocks [first_block_idx, first_block_idx + block_metas.len())
  -> one filter
```

It is not the current Hummock blocked-xor layout where each data block has its own raw filter. The filter can return false positives but must not return false negatives. Reusing a filter payload is in scope only for whole-shard raw copy in the first version.

The first v3 filter implementation should use one plain filter for the entire shard. It should not serialize N per-block filters inside a shard.

The initial shard size should be small enough to preserve fast-compaction raw-copy opportunities. The first experiments should start with 8 or 16 data blocks per shard, then compare larger sizes if object-store request count becomes the bottleneck.

## Serialized Format

The v3 serialized format is written in this order:

```text
data blocks
MetaShardBody[]
MetaPartitionIndexBody
checksum | version = 3 | magic
```

`MetaShardBody`:

```text
u32 block_meta_count
repeated BlockMeta
u32 filter_len
bytes filter_payload
```

`MetaPartitionIndexBody`:

```text
u32 estimated_size
u32 key_count
bytes smallest_key
bytes largest_key
u32 block_count
u32 shard_count
u32 filter_type
u32 shard_policy
repeated MetaShardDesc
```

`MetaShardDesc`:

```text
u32 first_block_idx
u32 block_count
bytes smallest_key
u64 offset
u32 len
u64 checksum
```

The footer checksum covers only `MetaPartitionIndexBody`. Each `MetaShardDesc.checksum` verifies its corresponding `MetaShardBody`.

The builder writes all data blocks first, then writes each `MetaShardBody`, records its descriptor, writes `MetaPartitionIndexBody`, and finally writes the footer with `version = 3`.

## v3 Read Path

The v3 point-read path should be:

```text
SstableInfo key-range/table-id prune
  -> get MetaPartitionIndex from meta-piece cache
  -> locate candidate MetaShard
  -> get MetaShard from meta-piece cache
  -> check MetaShard.filter
  -> if filter negative: skip
  -> if filter positive:
       locate data block in MetaShard.block_metas
       read data block
```

This makes filter checks possible without loading all block metadata or the full SST filter. Compared with separate index/filter partitions, this intentionally over-reads a small number of block metadata entries on filter-negative reads to avoid an extra object-store range request on filter-positive reads.

Range scans should use sequential shard prefetch to avoid turning one scan into one object-store request per shard. The prefetch policy is part of the IOPS / throughput tradeoff and should be tuned with workload measurements.

## Theoretical Read I/O

For a point read, assume `K` SSTs remain after `SstableInfo` key-range/table-id pruning and `P` of them are filter-positive.

| Layout | Metadata I/O on cold metadata cache | Metadata I/O when top-level/index is cached | Data block I/O |
|---|---:|---:|---:|
| Current Hummock full meta | `K` full-meta reads | `0` | `P` |
| v3 co-located `MetaShard` | `K` index + `K` meta shard | `K` meta shard | `P` |

The v3 design trades smaller metadata bytes and better cache granularity for shard-level metadata reads. With `MetaPartitionIndex` cached, the number of metadata range reads per candidate SST remains one, matching the current full-meta path while reading much less metadata. The design relies on keeping `MetaPartitionIndex` hot and using `get_or_fetch` to deduplicate concurrent misses.

RocksDB's partitioned index/filter has the same high-level tradeoff: it adds a small top-level index and loads only the required index/filter partitions on demand. If the top-level index is not cached, a lookup may need one additional metadata I/O before loading a partition. RocksDB recommends keeping top-level index/filter metadata cached or pinned with high priority.

The steady-state RocksDB point-lookup shape with partitioned index/filter is:

| RocksDB path | Metadata I/O with top-level index cached | Data block I/O |
|---|---:|---:|
| Filter negative | `1` filter partition | `0` |
| Filter positive | `1` filter partition + `1` index partition | `1` data block |

If top-level index/filter metadata is not cached, add one or two small top-level metadata reads depending on whether the filter top-level index and data-index top-level index are both missing. RocksDB is optimized for local block cache and disk; Hummock on object storage should treat those extra range requests as more expensive. This is why Hummock v3 co-locates block metadata and filter payload in one `MetaShard`.

## Cache Design

Data blocks stay in the existing data block cache. v3 metadata pieces should use a shared metadata-piece cache with two entry types:

```rust
enum MetaCacheKey {
    Index { sst_id: HummockSstableObjectId },
    Shard { sst_id: HummockSstableObjectId, shard_idx: u32 },
}

enum MetaCacheValue {
    Index(Box<MetaPartitionIndex>),
    Shard(Box<MetaShard>),
}
```

The public API should stay typed:

```rust
get_meta_index(sst_id)
get_meta_shard(sst_id, shard_idx)
```

This gives one shared metadata budget while keeping data blocks in a separate cache. It is similar to RocksDB's "typed blocks in one cache" model at the metadata level, but the physical metadata read unit is Hummock-specific.

Suggested priority:

| Entry type | Priority |
|---|---|
| `MetaPartitionIndex` | Highest |
| `MetaShard` | High / normal |

Concurrent misses for the same metadata piece should be deduplicated by `get_or_fetch`.

## Implementation Plan

The production implementation should keep v1/v2 readers on the legacy full-tail `SstableMeta` path and use the partitioned path for v3 SSTs.

Module-level changes:

| Module | Change |
|---|---|
| `sstable/mod.rs` | Add v3 `MetaPartitionIndex`, `MetaShard`, encode/decode, and footer version dispatch |
| `sstable/builder.rs` | Write data blocks, shard-level plain filters, `MetaShardBody[]`, and `MetaPartitionIndexBody` |
| `sstable_store.rs` | Load/cache v3 `MetaPartitionIndex`; add `get_meta_shard(sst_id, shard_idx)` range-read API |
| `Sstable` | Represent either legacy full `SstableMeta` or v3 partitioned index |
| `SstableIterator` / get path | Locate `MetaShard`, check shard filter, then use shard-local `BlockMeta` to read data blocks |
| metrics | Track index/shard cache hit rate, metadata bytes, metadata request count, and shard filter outcomes |

The implementation should avoid reconstructing a full `SstableMeta` for v3 SSTs.

## Fast Compaction

The preferred v3 fast-compaction unit is:

```text
whole shard = N raw data blocks + 1 MetaShard filter payload
```

Whole-shard raw copy is allowed only when a contiguous shard-sized run of input blocks satisfies the raw-copy conditions. For the first version, partial-shard raw copy should not reuse the shard filter payload.

When whole-shard raw copy succeeds, the output writer should:

- copy the N raw data block bytes,
- rewrite or regenerate the output `BlockMeta` entries because offsets change in the output SST,
- reuse the input `MetaShard` filter payload,
- encode a new output `MetaShard` from the rewritten `BlockMeta` entries and reused filter payload.

Whole-shard raw copy is ideal when:

- The output keeps all blocks in the input shard.
- The output shard boundary matches the input shard boundary.
- The filter type and layout are compatible.
- Data block bytes can be copied without recompression.
- Compaction filters, watermarks, table-id pruning, and vnode/key-range pruning do not remove keys inside the shard.

Partial-shard cases need a fallback policy:

| Case | Policy |
|---|---|
| Output keeps a subset of one input shard | Fallback to decoded/rewrite path and rebuild the output filter |
| Output mixes blocks from multiple input shards | Rebuild the output filter, or cut output at source shard boundaries |
| Output contains newly decoded or rewritten keys | Rebuild the output filter |
| Kept subset is too small | Prefer rebuilding the output filter to avoid poor false-positive rate |

RocksDB usually rebuilds filters while writing a new SST during compaction. Hummock fast compaction has a stronger raw-copy requirement, so it needs explicit whole-shard reuse and partial fallback rules.

## Compatibility

- v1/v2 SSTs continue to use the legacy full-tail `SstableMeta` decode path.
- v3 SSTs use the partitioned metadata path.
- New binaries must read v1/v2/v3.
- Old binaries are not required to read v3.
- Version dispatch should use the existing footer pattern instead of heuristic decoding.

## Open Questions

- Default shard size: start with 8 or 16 blocks, then evaluate larger sizes if metadata IOPS is too high.
- Whether table-id boundaries should be hard shard boundaries. The current direction is yes.
- Whether vnode boundaries should be soft boundaries. The current direction is yes.
- Whether `MetaShardDesc.smallest_key` should be stored redundantly or derived through an index into the first `BlockMeta` of the shard.
- Whether filter builder memory needs additional limits for shard-level plain filters.
- What false-positive-rate threshold should trigger partial-shard filter rebuild if partial reuse is later allowed.
- What scan prefetch depth minimizes object-store request count without over-reading if benchmarks show IOPS pressure.
- What metrics should be added for consecutive raw-copy run length and shard fallback reasons.

## Metrics Needed Before Finalizing Parameters

- Total filter bytes per SST.
- Total block metadata bytes per SST.
- Distribution of block count per SST.
- Distribution of consecutive raw-copy block runs in fast compaction.
- Filter hit / negative ratio for point reads with prefix hints.
- Object-store request count and bytes read for point reads and scans under different `MetaShard` sizes.
