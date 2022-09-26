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

pub mod event_handler;
pub mod memtable;
pub mod state_store;
pub mod version;

use std::ops::RangeBounds;

use bytes::Bytes;
use futures::Future;
use risingwave_common::catalog::TableId;

use crate::error::StorageResult;
use crate::storage_value::StorageValue;
use crate::StateStoreIter;

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send;
pub trait IterFutureTrait<'a, I: StateStoreIter<Item = (Bytes, Bytes)>, R, B> =
    Future<Output = StorageResult<I>> + Send;
pub trait IngestKVBatchFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send;

#[derive(Default, Clone)]
pub struct WriteOptions {
    pub epoch: u64,
    pub table_id: TableId,
}

#[macro_export]
macro_rules! define_local_state_store_associated_type {
    () => {
        type IngestKVBatchFuture<'a> = impl IngestKVBatchFutureTrait<'a>;
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IterFuture<'a, R, B>  = impl IterFutureTrait<'a, Self::Iter, R, B>
                                                            where
                                                                R: 'static + Send + RangeBounds<B>,
                                                                B: 'static + Send + AsRef<[u8]>;
    };
}

/// State store v2.
/// It provides the basic functionalities streaming/batch executor needs to access the underlying
/// state store.
pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter: StateStoreIter<Item = (Bytes, Bytes)>;

    type GetFuture<'a>: GetFutureTrait<'a>;

    type IterFuture<'a, R, B>: IterFutureTrait<'a, Self::Iter, R, B>
    where
        R: 'static + Send + RangeBounds<B>,
        B: 'static + Send + AsRef<[u8]>;

    type IngestKVBatchFuture<'a>: IngestKVBatchFutureTrait<'a>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_>;

    /// Opens and returns an iterator for a given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to
    /// the given `epoch`.
    fn iter<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Inserts a key-value entry associated with a given `epoch` into the state store.
    fn insert(&self, key: Bytes, val: Bytes) -> StorageResult<()>;

    /// Deletes a key-value entry from the state store. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&self, key: Bytes) -> StorageResult<()>;

    /// Triggers a flush to persistent storage for the in-memory states.
    fn flush(&self) -> StorageResult<usize>;

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestKVBatchFuture<'_>;

    /// Updates the monotonically increasing write epoch to `new_epoch`.
    /// All writes after this function is called will be tagged with `new_epoch`. In other words,
    /// the previous write epoch is sealed.
    fn advance_write_epoch(&mut self, new_epoch: u64) -> StorageResult<()>;
}

#[allow(unused)]
#[derive(Default, Clone)]
pub struct ReadOptions {
    /// A hint for prefix key to check bloom filter.
    /// If the `prefix_hint` is not None, it should be included in
    /// `key` or `key_range` in the read API.
    prefix_hint: Option<Vec<u8>>,
    check_bloom_filter: bool,
    pub retention_seconds: Option<u32>,
}
