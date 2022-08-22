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

pub mod memtable;
pub mod store;
pub mod version;
pub mod write_queue;

use std::ops::RangeBounds;

use bytes::Bytes;
use futures::Future;
use risingwave_hummock_sdk::LocalSstableInfo;

use crate::error::StorageResult;
use crate::StateStoreIter;

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send;
pub trait IterFutureTrait<'a, I: StateStoreIter<Item = (Bytes, Bytes)>, R, B> =
    Future<Output = StorageResult<I>> + Send;
pub trait EmptyFutureTrait<'a> = Future<Output = StorageResult<()>> + Send;
pub trait FlushFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send;
pub trait SyncFutureTrait<'a> =
    Future<Output = StorageResult<(usize, Vec<LocalSstableInfo>)>> + Send;

#[macro_export]
macro_rules! define_local_state_store_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IterFuture<'a, R, B>  = impl IterFutureTrait<'a, Self::Iter, R, B>
                                                            where
                                                                R: 'static + Send + RangeBounds<B>,
                                                                B: 'static + Send + AsRef<[u8]>;
        type InsertFuture<'a> = impl EmptyFutureTrait<'a>;
        type DeleteFuture<'a> = impl EmptyFutureTrait<'a>;
        type FlushFuture<'a> = impl FlushFutureTrait<'a>;
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

    type InsertFuture<'a>: EmptyFutureTrait<'a>;

    type DeleteFuture<'a>: EmptyFutureTrait<'a>;

    type FlushFuture<'a>: FlushFutureTrait<'a>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get(&self, key: &[u8], epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_>;

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
    fn insert(&self, key: Bytes, val: Bytes, epoch: u64) -> Self::InsertFuture<'_>;

    /// Deletes a key-value entry from the state store. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&self, key: Bytes, epoch: u64) -> Self::DeleteFuture<'_>;

    /// Triggers a flush to persistent storage for the in-memory states.
    fn flush(&self) -> Self::FlushFuture<'_>;
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
