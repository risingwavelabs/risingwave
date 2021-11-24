//! Hummock is the state store of the streaming system.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use iterator::*;
mod table;
use table::*;
mod bloom;
use bloom::*;
mod cloud;
mod error;
mod format;
mod iterator;
mod keyed_state;
mod value;
use value::*;

#[cfg(test)]
mod mock_fs;

use crate::storage::hummock::cloud::upload;
pub use error::*;

#[derive(Default, Debug, Clone)]
pub struct HummockOptions {
    /// target size of the table
    pub table_size: u32,
    /// size of each block in bytes in SST
    pub block_size: u32,
    /// false positive probability of Bloom filter
    pub bloom_false_positive: f64,
}

/// Hummock is the state store backend.
pub struct HummockStorage {
    options: Arc<HummockOptions>,
    unique_id: AtomicU64,
}

impl HummockStorage {
    pub fn new(options: HummockOptions) -> Self {
        Self {
            options: Arc::new(options),
            unique_id: AtomicU64::new(0),
        }
    }

    /// Write batch to storage.
    pub async fn write_batch(
        &mut self,
        kv_pairs: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> HummockResult<()> {
        let get_builder = |options: &HummockOptions| {
            TableBuilder::new(TableBuilderOptions {
                table_capacity: options.table_size,
                block_size: options.block_size,
                bloom_false_positive: options.bloom_false_positive,
            })
        };

        let mut table_builder = get_builder(&self.options);
        for (k, v) in kv_pairs {
            table_builder.add(k.as_slice(), v);
        }

        // Producing only one table regardless of capacity for now.
        // TODO: update kv pairs to multi tables when size of the kv pairs is larger than
        // TODO: the capacity of a single table.
        let (blocks, meta) = table_builder.finish();
        let old_thread_count = self.unique_id.fetch_add(1, Ordering::SeqCst);
        let table = Table::load(old_thread_count, blocks, meta).unwrap();

        // upload table to cloud
        let _loc_url = upload(table, &self.options);
        Ok(())
    }
}
