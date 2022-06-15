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
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::range_of_prefix;

use crate::error::StorageResult;
use crate::{StateStore, StateStoreIter};

/// Provides API to read key-value pairs of a prefix in the storage backend.
#[derive(Clone)]
pub struct Keyspace<S: StateStore> {
    store: S,

    /// Encoded representation for all segments.
    prefix: Vec<u8>,
}

impl<S: StateStore> Keyspace<S> {
    /// Creates a shared root [`Keyspace`] for all executors of the same operator.
    ///
    /// By design, all executors of the same operator should share the same keyspace in order to
    /// support scaling out, and ensure not to overlap with each other. So we use `table_id`
    /// here.

    /// Creates a root [`Keyspace`] for a table.
    pub fn table_root(store: S, id: &TableId) -> Self {
        let prefix = {
            let mut buf = BytesMut::with_capacity(5);
            buf.put_u8(b't');
            buf.put_u32(id.table_id);
            buf.to_vec()
        };
        Self { store, prefix }
    }

    /// Appends more bytes to the prefix and returns a new `Keyspace`
    #[must_use]
    pub fn append(&self, mut bytes: Vec<u8>) -> Self {
        let mut prefix = self.prefix.clone();
        prefix.append(&mut bytes);
        Self {
            store: self.store.clone(),
            prefix,
        }
    }

    #[must_use]
    pub fn append_u8(&self, val: u8) -> Self {
        self.append(val.to_be_bytes().to_vec())
    }

    #[must_use]
    pub fn append_u16(&self, val: u16) -> Self {
        self.append(val.to_be_bytes().to_vec())
    }

    /// Treats the keyspace as a single key, and returns the key.
    pub fn key(&self) -> &[u8] {
        &self.prefix
    }

    /// Treats the keyspace as a single key, and gets its value.
    /// The returned value is based on a snapshot corresponding to the given `epoch`
    pub async fn value(&self, epoch: u64) -> StorageResult<Option<Bytes>> {
        self.store.get(&self.prefix, epoch).await
    }

    /// Concatenates this keyspace and the given key to produce a prefixed key.
    pub fn prefixed_key(&self, key: impl AsRef<[u8]>) -> Vec<u8> {
        [self.prefix.as_slice(), key.as_ref()].concat()
    }

    /// Gets from the keyspace with the `prefixed_key` of given key.
    /// The returned value is based on a snapshot corresponding to the given `epoch`
    pub async fn get(&self, key: impl AsRef<[u8]>, epoch: u64) -> StorageResult<Option<Bytes>> {
        self.store.get(&self.prefixed_key(key), epoch).await
    }

    /// Scans `limit` keys from the keyspace and get their values. If `limit` is None, all keys of
    /// the given prefix will be scanned. Note that the prefix of this keyspace will be stripped.
    /// The returned values are based on a snapshot corresponding to the given `epoch`
    pub async fn scan(
        &self,
        limit: Option<usize>,
        epoch: u64,
    ) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let range = range_of_prefix(&self.prefix);
        let mut pairs = self.store.scan(range, limit, epoch).await?;
        pairs
            .iter_mut()
            .for_each(|(k, _v)| *k = k.slice(self.prefix.len()..));
        Ok(pairs)
    }

    /// Gets an iterator with the prefix of this keyspace.
    /// The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    async fn iter_inner(&'_ self, epoch: u64) -> StorageResult<S::Iter> {
        let range = range_of_prefix(&self.prefix);
        self.store.iter(range, epoch).await
    }

    pub async fn iter(&self, epoch: u64) -> StorageResult<StripPrefixIterator<S::Iter>> {
        let iter = self.iter_inner(epoch).await?;
        let strip_prefix_iterator = StripPrefixIterator {
            iter,
            prefix_len: self.prefix.len(),
        };
        Ok(strip_prefix_iterator)
    }

    pub async fn iter_with_range<R, B>(
        &self,
        pk_bounds: R,
        epoch: u64,
    ) -> StorageResult<StripPrefixIterator<S::Iter>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let start = match pk_bounds.start_bound() {
            Included(k) => Included(Bytes::copy_from_slice(k.as_ref())),
            Excluded(k) => Excluded(Bytes::copy_from_slice(k.as_ref())),
            Unbounded => Unbounded,
        };
        let end = match pk_bounds.end_bound() {
            Included(k) => Included(Bytes::copy_from_slice(k.as_ref())),
            Excluded(k) => Excluded(Bytes::copy_from_slice(k.as_ref())),
            Unbounded => Unbounded,
        };
        let range = (start, end);
        let iter = self.store.iter(range, epoch).await?;
        let strip_prefix_iterator = StripPrefixIterator {
            iter,
            prefix_len: self.prefix.len(),
        };
        Ok(strip_prefix_iterator)
    }

    /// Gets the underlying state store.
    pub fn state_store(&self) -> S {
        self.store.clone()
    }
}

pub struct StripPrefixIterator<I: StateStoreIter<Item = (Bytes, Bytes)>> {
    iter: I,
    prefix_len: usize,
}

impl<I: StateStoreIter<Item = (Bytes, Bytes)>> StateStoreIter for StripPrefixIterator<I> {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> =
        impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            Ok(self
                .iter
                .next()
                .await?
                .map(|(key, value)| (key.slice(self.prefix_len..), value)))
        }
    }
}
