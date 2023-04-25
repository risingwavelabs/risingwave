use bincode::{Decode, Encode};
use bytes::Bytes;
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::{TableId, TableOption};

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Encode, Decode)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill(CachePriority),
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::Fill(CachePriority::High)
    }
}

/// If `exhaust_iter` is true, prefetch will be enabled. Prefetching may increase the memory
/// footprint of the CN process because the prefetched blocks cannot be evicted.
#[derive(Debug, Default, Clone, Copy, Encode, Decode, PartialEq, Eq)]
pub struct PrefetchOptions {
    /// `exhaust_iter` is set `true` only if the return value of `iter()` will definitely be
    /// exhausted, i.e., will iterate until end.
    pub exhaust_iter: bool,
}

impl PrefetchOptions {
    pub fn new_for_exhaust_iter() -> Self {
        Self { exhaust_iter: true }
    }
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    /// A hint for prefix key to check bloom filter.
    /// If the `prefix_hint` is not None, it should be included in
    /// `key` or `key_range` in the read API.
    pub prefix_hint: Option<Bytes>,
    pub ignore_range_tombstone: bool,
    pub prefetch_options: PrefetchOptions,
    pub cache_policy: CachePolicy,

    pub retention_seconds: Option<u32>,
    pub table_id: TableId,
    /// Read from historical hummock version of meta snapshot backup.
    /// It should only be used by `StorageTable` for batch query.
    pub read_version_from_backup: bool,
}

impl ReadOptions {
    pub fn for_test(table_id: u32) -> Self {
        Self {
            prefix_hint: Some(Bytes::from(vec![0])),
            ignore_range_tombstone: true,
            prefetch_options: PrefetchOptions { exhaust_iter: true },
            cache_policy: CachePolicy::Disable,
            retention_seconds: None,
            table_id: TableId { table_id: table_id },
            read_version_from_backup: true,
        }
    }
}

#[derive(Debug, Default, Clone, Encode, Decode, PartialEq)]
pub struct WriteOptions {
    pub epoch: u64,
    pub table_id: TableId,
}

#[derive(Clone, Default, Copy, Encode, Decode, Debug, PartialEq, Eq)]
pub struct NewLocalOptions {
    pub table_id: TableId,
    /// Whether the operation is consistent. The term `consistent` requires the following:
    ///
    /// 1. A key cannot be inserted or deleted for more than once, i.e. inserting to an existing
    /// key or deleting an non-existing key is not allowed.
    ///
    /// 2. The old value passed from
    /// `update` and `delete` should match the original stored value.
    pub is_consistent_op: bool,
    pub table_option: TableOption,
}

impl NewLocalOptions {
    pub fn for_test(table_id: TableId) -> Self {
        Self {
            table_id,
            is_consistent_op: false,
            table_option: TableOption {
                retention_seconds: None,
            },
        }
    }
}
