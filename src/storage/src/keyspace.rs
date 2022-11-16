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

use std::future::Future;
use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;

use crate::error::StorageResult;
use crate::store::{ReadOptions, StateStoreRead, StateStoreReadExt, StateStoreWrite, WriteOptions};
use crate::write_batch::KeySpaceWriteBatch;
use crate::StateStoreIter;

/// Provides API to read key-value pairs of a prefix in the storage backend.
#[derive(Clone)]
pub struct Keyspace<S> {
    store: S,

    /// Encoded representation for all segments.
    prefix: Vec<u8>,

    table_id: TableId,
}

// TODO: remove storage interface from keyspace, and and call it directly in storage_table
impl<S> Keyspace<S> {
    /// Creates a shared root [`Keyspace`] for all executors of the same operator.
    ///
    /// By design, all executors of the same operator should share the same keyspace in order to
    /// support scaling out, and ensure not to overlap with each other. So we use `table_id`
    /// here.

    /// Creates a root [`Keyspace`] for a table.
    pub fn table_root(store: S, id: TableId) -> Self {
        let prefix = id.table_id().to_be_bytes().to_vec();
        Self {
            store,
            prefix,
            table_id: id,
        }
    }

    /// Concatenates this keyspace and the given key to produce a prefixed key.
    pub fn prefixed_key(&self, key: impl AsRef<[u8]>) -> Vec<u8> {
        [self.prefix.as_slice(), key.as_ref()].concat()
    }

    /// Gets the underlying state store.
    pub fn state_store(&self) -> &S {
        &self.store
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl<S: StateStoreRead> Keyspace<S> {
    /// Gets from the keyspace with the `prefixed_key` of given key.
    /// The returned value is based on a snapshot corresponding to the given `epoch`.
    pub async fn get(
        &self,
        key: impl AsRef<[u8]>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        self.store.get(key.as_ref(), epoch, read_options).await
    }

    /// Scans `limit` keys from the keyspace and get their values.
    /// If `limit` is None, all keys of the given prefix will be scanned.
    /// The returned values are based on a snapshot corresponding to the given `epoch`.
    pub async fn scan(
        &self,
        epoch: u64,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> StorageResult<Vec<(Vec<u8>, Bytes)>> {
        self.scan_with_range::<_, &[u8]>(.., epoch, limit, read_options)
            .await
    }

    /// Scans `limit` keys from the given `range` in this keyspace and get their values.
    /// If `limit` is None, all keys of the given prefix will be scanned.
    /// The returned values are based on a snapshot corresponding to the given `epoch`.
    ///
    /// **Note**: the `range` should not be prepended with the prefix of this keyspace.
    pub async fn scan_with_range<R, B>(
        &self,
        range: R,
        epoch: u64,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> StorageResult<Vec<(Vec<u8>, Bytes)>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let range = to_owned_range(range);
        let pairs = self.store.scan(range, epoch, limit, read_options).await?;
        let pairs = pairs
            .into_iter()
            .map(|(k, v)| (k.user_key.table_key.0, v))
            .collect();
        Ok(pairs)
    }

    /// Gets an iterator of this keyspace.
    /// The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    pub async fn iter(
        &self,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<ExtractTableKeyIterator<S::Iter>> {
        self.iter_with_range::<_, &[u8]>(.., epoch, read_options)
            .await
    }

    /// Gets an iterator of the given `range` in this keyspace.
    /// The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    ///
    /// **Note**: the `range` should not be prepended with the prefix of this keyspace.
    pub async fn iter_with_range<R, B>(
        &self,
        range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<ExtractTableKeyIterator<S::Iter>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let range = to_owned_range(range);
        let iter = self.store.iter(range, epoch, read_options).await?;
        let extract_table_key_iter = ExtractTableKeyIterator { iter };

        Ok(extract_table_key_iter)
    }
}

impl<S: StateStoreWrite> Keyspace<S> {
    pub fn start_write_batch(&self, option: WriteOptions) -> KeySpaceWriteBatch<'_, S> {
        let write_batch = self.store.start_write_batch(option);
        write_batch.prefixify()
    }
}

fn to_owned_range<R, B>(range: R) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
where
    R: RangeBounds<B>,
    B: AsRef<[u8]>,
{
    (
        range.start_bound().map(|b| b.as_ref().to_vec()),
        range.end_bound().map(|b| b.as_ref().to_vec()),
    )
}

pub struct ExtractTableKeyIterator<I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)> + 'static> {
    iter: I,
}

impl<I: StateStoreIter<Item = (FullKey<Vec<u8>>, Bytes)>> StateStoreIter
    for ExtractTableKeyIterator<I>
{
    type Item = (Vec<u8>, Bytes);

    type NextFuture<'a> =
        impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> + Send + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            Ok(self
                .iter
                .next()
                .await?
                .map(|(key, value)| (key.user_key.table_key.0, value)))
        }
    }
}
