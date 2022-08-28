// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;

use super::event_handler::HummockEvent;
use super::version::{HummockReadVersion, VersionUpdate};
use super::{GetFutureTrait, IterFutureTrait, ReadOptions, StateStore};
use crate::define_local_state_store_associated_type;
use crate::error::StorageResult;
use crate::hummock::{HummockResult, HummockStateStoreIter};
use crate::table::streaming_table::mem_table::MemTable;

#[allow(unused)]
pub struct HummockStorageCore {
    /// Mutable memtable.
    memtable: MemTable,

    /// Read handle.
    read_version: HummockReadVersion,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,
}

#[allow(unused)]
#[derive(Clone)]
pub struct HummockStorage {
    core: Arc<Mutex<HummockStorageCore>>,
}

#[allow(unused)]
impl HummockStorage {
    /// See `HummockReadVersion::update` for more details.
    pub fn update(&mut self, info: VersionUpdate) -> HummockResult<()> {
        unimplemented!()
    }
}

#[allow(unused)]
impl StateStore for HummockStorage {
    type Iter = HummockStateStoreIter;

    define_local_state_store_associated_type!();

    fn insert(&self, key: Bytes, val: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn delete(&self, key: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn get(&self, key: &[u8], epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn flush(&self) -> StorageResult<usize> {
        unimplemented!()
    }

    fn advance_write_epoch(&mut self, new_epoch: u64) -> StorageResult<()> {
        unimplemented!()
    }
}
