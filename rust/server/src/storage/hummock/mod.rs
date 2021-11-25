//! Hummock is the state store of the streaming system.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use iterator::*;
mod table;
use table::*;
mod bloom;
use bloom::*;
mod cloud;
mod error;
mod iterator;
mod keyed_state;
mod value;
use value::*;

use crate::storage::hummock::cloud::gen_remote_table;
use crate::storage::object::ObjectStore;
pub use error::*;

pub static REMOTE_DIR: &str = "/test/";

#[derive(Default, Debug, Clone)]
pub struct HummockOptions {
    /// target size of the table
    pub table_size: u32,
    /// size of each block in bytes in SST
    pub block_size: u32,
    /// false positive probability of Bloom filter
    pub bloom_false_positive: f64,
    /// remote direcotry for storing data and metadata objects
    pub remote_dir: String,
}

/// Hummock is the state store backend.
pub struct HummockStorage {
    options: Arc<HummockOptions>,
    unique_id: AtomicU64,
    tables: HashMap<u64, Table>,
    obj_client: Arc<dyn ObjectStore>,
}

impl HummockStorage {
    pub fn new(obj_client: Arc<dyn ObjectStore>, options: HummockOptions) -> Self {
        Self {
            options: Arc::new(options),
            unique_id: AtomicU64::new(0),
            tables: HashMap::new(),
            obj_client,
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
        let table_id = self.unique_id.fetch_add(1, Ordering::SeqCst);
        let remote_dir = Some(self.options.remote_dir.as_str());
        let table =
            gen_remote_table(self.obj_client.clone(), table_id, blocks, meta, remote_dir).await?;

        self.tables.insert(table_id, table);
        Ok(())
    }
}

/// `assert_eq` two `Vec<u8>` with human-readable format.
#[macro_export]
macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {{
        use bytes::Bytes;
        assert_eq!(
            Bytes::copy_from_slice(&$left),
            Bytes::copy_from_slice(&$right)
        )
    }};
}
