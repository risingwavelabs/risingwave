//! Hummock state store's SST builder, format and iterator

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod block;
pub use block::*;
mod block_iterator;
mod bloom;
use bloom::Bloom;
pub mod builder;
pub mod multi_builder;
pub use block_iterator::*;
pub use builder::*;
mod sstable_iterator;
pub use sstable_iterator::*;
mod reverse_sstable_iterator;
pub use reverse_sstable_iterator::*;
mod utils;

use risingwave_pb::hummock::SstableMeta;

use super::{HummockError, HummockResult};

/// [`SSTable`] is a handle for accessing SST in [`TableManager`].
pub struct Sstable {
    pub id: u64,
    pub meta: SstableMeta,
}

impl Sstable {
    pub fn new(id: u64, meta: SstableMeta) -> Self {
        Self { id, meta }
    }

    pub fn has_bloom_filter(&self) -> bool {
        !self.meta.bloom_filter.is_empty()
    }

    pub fn surely_not_have_user_key(&self, user_key: &[u8]) -> bool {
        if self.has_bloom_filter() {
            let hash = farmhash::fingerprint32(user_key);
            let bloom = Bloom::new(&self.meta.bloom_filter);
            bloom.surely_not_have_hash(hash)
        } else {
            false
        }
    }

    pub fn block_count(&self) -> usize {
        self.meta.block_metas.len()
    }
}
