//! Hummock is the state store of the streaming system.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod table;
pub use table::*;
mod cloud;
mod error;
mod iterator;
mod value;
use self::table::format::key_with_ts;
use crate::object::ObjectStore;
use cloud::gen_remote_table;
pub use error::*;
use tokio::sync::Mutex;
use value::*;

pub static REMOTE_DIR: &str = "/test/";

#[derive(Default, Debug, Clone)]
pub struct HummockOptions {
    /// target size of the table
    pub table_size: u32,
    /// size of each block in bytes in SST
    pub block_size: u32,
    /// false positive probability of Bloom filter
    pub bloom_false_positive: f64,
    /// remote directory for storing data and metadata objects
    pub remote_dir: String,
}

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<HummockOptions>,
    unique_id: Arc<AtomicU64>,
    tables: Arc<Mutex<HashMap<u64, Table>>>,
    obj_client: Arc<dyn ObjectStore>,
}

impl HummockStorage {
    pub fn new(obj_client: Arc<dyn ObjectStore>, options: HummockOptions) -> Self {
        Self {
            options: Arc::new(options),
            unique_id: Arc::new(AtomicU64::new(0)),
            tables: Arc::new(Mutex::new(HashMap::new())),
            obj_client,
        }
    }

    pub async fn get(&self, _key: &[u8]) -> HummockResult<Option<Vec<u8>>> {
        todo!()
    }

    /// Write batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    /// * Globally unique. The streaming operators should ensure that different operators won't
    ///   operate on the same key. The operator operating on one keyspace should always wait for all
    ///   changes to be committed before reading and writing new keys to the engine. That is because
    ///   that the table with lower epoch might be committed after a table with higher epoch has
    ///   been committed. If such case happens, the outcome is non-predictable.
    pub async fn write_batch(
        &self,
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
            let k = key_with_ts(k, 0);
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

        self.tables.lock().await.insert(table_id, table);
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
