//! Hummock is the state store of the streaming system.

mod table;
use std::sync::Arc;

pub use table::*;
mod bloom;
mod error;
mod format;
mod keyed_state;

pub use error::*;

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<HummockOptions>,
}

pub enum HummockValue<T> {
    Put(T),
    Delete,
}

#[derive(Default)]
pub struct HummockOptions;

impl HummockStorage {
    pub fn new(options: HummockOptions) -> Self {
        Self {
            options: Arc::new(options),
        }
    }

    /// Ingest KV pairs into the storage engine
    pub async fn ingest_kv(
        &self,
        _kv_pairs: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> HummockResult<()> {
        todo!()
    }
}
