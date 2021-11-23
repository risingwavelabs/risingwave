//! Hummock is the state store of the streaming system.

mod table;
use bytes::{Buf, BufMut};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
pub use table::*;
mod bloom;
mod cloud;
mod error;
mod format;
mod keyed_state;

#[cfg(test)]
mod mock_fs;

use crate::storage::hummock::cloud::upload;
pub use error::*;

pub const VALUE_DELETE: u8 = 1 << 0;
pub const VALUE_PUT: u8 = 0;

#[derive(Debug)]
pub enum HummockValue<T> {
    Put(T),
    Delete,
}

impl PartialEq for HummockValue<Vec<u8>> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Put(l0), Self::Put(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl HummockValue<Vec<u8>> {
    pub fn encoded_len(&self) -> usize {
        match self {
            HummockValue::Put(val) => 1 + val.len(),
            HummockValue::Delete => 1,
        }
    }

    /// Encode the object
    pub fn encode(&self, buffer: &mut impl BufMut) {
        match self {
            HummockValue::Put(val) => {
                // set flag
                buffer.put_u8(VALUE_PUT);
                buffer.put_slice(val.as_slice());
            }
            HummockValue::Delete => {
                // set flag
                buffer.put_u8(VALUE_DELETE);
            }
        }
    }

    /// Decode the object
    pub fn decode(buffer: &mut impl Buf) -> HummockResult<Self> {
        if buffer.remaining() == 0 {
            return Err(HummockError::DecodeError("empty value".to_string()));
        }
        match buffer.get_u8() {
            VALUE_PUT => Ok(Self::Put(Vec::from(buffer.chunk()))),
            VALUE_DELETE => Ok(Self::Delete),
            _ => Err(HummockError::DecodeError(
                "non-empty but format error".to_string(),
            )),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct HummockOptions {
    /// target size of the table
    pub table_size: u32,
    /// size of each block in bytes in SST
    pub block_size: u32,
    /// false positive probability of bloom filter
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
                table_size: options.table_size,
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
        let (blocks, meta) = table_builder.finish_to_blocks_and_meta();
        let old_thread_count = self.unique_id.fetch_add(1, Ordering::SeqCst);
        let table = Table::load(old_thread_count, blocks, meta).unwrap();

        // upload table to cloud
        let _loc_url = upload(table, &self.options);
        Ok(())
    }
}
