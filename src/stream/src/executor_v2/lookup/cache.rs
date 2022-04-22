use std::collections::BTreeSet;

use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::collection::evictable::EvictableHashMap;

/// A cache for lookup's arrangement side.
pub struct LookupCache {
    data: EvictableHashMap<Row, BTreeSet<Row>>,
}

impl LookupCache {
    /// Lookup a row in cache. If not found, return `None`.
    pub fn lookup(&mut self, key: &Row) -> Option<&BTreeSet<Row>> {
        self.data.get(key)
    }

    /// Update a key after lookup cache misses.
    pub fn batch_update(&mut self, key: Row, value: impl Iterator<Item = Row>) {
        self.data.push(key, value.collect());
    }

    /// Apply a batch from the arrangement side
    pub fn apply_batch(&mut self, chunk: StreamChunk, arrange_join_keys: &[usize]) {
        for (op, row) in chunk.rows() {
            let key = row.row_by_indices(arrange_join_keys);
            if let Some(values) = self.data.get_mut(&key) {
                // the item is in cache, update it
                let value = row.to_owned_row();
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        values.insert(value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        values.remove(&value);
                    }
                }
            }
        }
    }

    /// Flush the cache and evict the items.
    pub fn flush(&mut self) {
        self.data.evict_to_target_cap();
    }

    pub fn new() -> Self {
        Self {
            data: EvictableHashMap::new(1 << 16),
        }
    }
}
