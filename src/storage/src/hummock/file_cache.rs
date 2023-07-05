// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::Hash;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use foyer::common::code::{Key, Value};
use risingwave_hummock_sdk::HummockSstableObjectId;

use super::Block;

#[derive(thiserror::Error, Debug)]
pub enum FileCacheError {
    #[error("foyer error: {0}")]
    Foyer(#[from] foyer::storage::error::Error),
}

impl FileCacheError {
    fn foyer(e: foyer::storage::error::Error) -> Self {
        Self::Foyer(e)
    }
}

pub type Result<T> = core::result::Result<T, FileCacheError>;

pub type Admission = foyer::storage::admission::AdmitAll<SstableBlockIndex, Box<Block>>;
pub type Reinsertion = foyer::storage::reinsertion::ReinsertNone<SstableBlockIndex, Box<Block>>;
pub type EvictionConfig = foyer::intrusive::eviction::lfu::LfuConfig;
pub type DeviceConfig = foyer::storage::device::fs::FsDeviceConfig;

pub type FoyerStore =
    foyer::storage::LfuFsStore<SstableBlockIndex, Box<Block>, Admission, Reinsertion>;

pub type FoyerStoreConfig = foyer::storage::LfuFsStoreConfig<Admission, Reinsertion>;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct SstableBlockIndex {
    pub sst_id: HummockSstableObjectId,
    pub block_idx: u64,
}

impl Key for SstableBlockIndex {
    fn serialized_len(&self) -> usize {
        8 + 8
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.sst_id);
        buf.put_u64(self.block_idx);
    }

    fn read(mut buf: &[u8]) -> Self {
        let sst_id = buf.get_u64();
        let block_idx = buf.get_u64();
        Self { sst_id, block_idx }
    }
}

impl Value for Box<Block> {
    fn serialized_len(&self) -> usize {
        self.data().len()
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_slice(self.data())
    }

    fn read(buf: &[u8]) -> Self {
        let data = Bytes::copy_from_slice(buf);
        let block = Block::decode_from_raw(data);
        Box::new(block)
    }
}

#[derive(Clone)]
pub enum FileCache {
    None,
    Foyer(Arc<FoyerStore>),
}

impl FileCache {
    pub fn none() -> Self {
        Self::None
    }

    pub async fn foyer(config: FoyerStoreConfig) -> Result<Self> {
        let store = FoyerStore::open(config)
            .await
            .map_err(FileCacheError::foyer)?;
        Ok(Self::Foyer(store))
    }

    pub async fn insert(&self, key: SstableBlockIndex, value: Box<Block>) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::Foyer(store) => store
                .insert(key, value)
                .await
                .map(|_| ())
                .map_err(FileCacheError::foyer),
        }
    }

    pub fn remove(&self, key: &SstableBlockIndex) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::Foyer(store) => {
                store.remove(key);
                Ok(())
            }
        }
    }

    pub async fn lookup(&self, key: &SstableBlockIndex) -> Result<Option<Box<Block>>> {
        match self {
            FileCache::None => Ok(None),
            FileCache::Foyer(store) => store.lookup(key).await.map_err(FileCacheError::foyer),
        }
    }
}
