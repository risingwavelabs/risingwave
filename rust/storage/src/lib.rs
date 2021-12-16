#![allow(dead_code)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![deny(unused_must_use)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(drain_filter)]

pub mod bummock;
pub mod hummock;
pub mod memory;
pub mod object;
pub mod panic_store;
pub mod table;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::error::Result;
use risingwave_common::types::DataTypeRef;

use crate::table::ScannableTable;

#[async_trait]
pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter: StateStoreIter<Item = (Bytes, Bytes)>;

    /// Point get a value from the state store.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Scan `limit` number of keys from the keyspace. If `limit` is `None`, scan all elements.
    ///
    /// TODO: this interface should be refactored to return an iterator in the future. And in some
    /// cases, the scan can be optimized into a `multi_get` request.
    async fn scan(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>>;

    /// Ingest a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()>;

    fn iter(&self, prefix: &[u8]) -> Self::Iter;
}

#[async_trait]
pub trait StateStoreIter: Send + Sync + 'static {
    type Item;

    async fn open(&mut self) -> Result<()>;

    async fn next(&mut self) -> Result<Option<Self::Item>>;
}

/// Provides API to read key-value pairs of a prefix in the storage backend.
#[derive(Clone)]
pub struct Keyspace<S: StateStore> {
    store: S,
    prefix: Vec<u8>,
}

impl<S: StateStore> Keyspace<S> {
    pub fn new(store: S, prefix: Vec<u8>) -> Self {
        Self { store, prefix }
    }

    /// Get the key from the keyspace
    pub async fn get(&self) -> Result<Option<Bytes>> {
        self.store.get(&self.prefix).await
    }

    /// Scan `limit` keys from the keyspace. If `limit` is None, all keys of the given prefix will
    /// be returned.
    pub async fn scan(&self, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        self.store.scan(&self.prefix, limit).await
    }

    /// Scan from the keyspace, and then strip the prefix of this keyspace.
    ///
    /// See also: [`Keyspace::scan`]
    pub async fn scan_strip_prefix(&self, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let mut pairs = self.scan(limit).await?;
        pairs
            .iter_mut()
            .for_each(|(k, _v)| *k = k.slice(self.prefix.len()..));
        Ok(pairs)
    }

    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Concatenate prefix of this keyspace and the given key to produce a prefixed key.
    pub fn prefixed_key(&self, key: impl AsRef<[u8]>) -> Vec<u8> {
        [self.prefix.as_slice(), key.as_ref()].concat()
    }

    /// Get the underlying state store.
    pub fn state_store(&self) -> S {
        self.store.clone()
    }

    /// Get a sub-keyspace.
    pub fn keyspace(&self, sub_prefix: &[u8]) -> Self {
        Self {
            store: self.store.clone(),
            prefix: [&self.prefix[..], sub_prefix].concat(),
        }
    }
}

/// `Table` is an abstraction of the collection of columns and rows.
/// Each `Table` can be viewed as a flat sheet of a user created table.
#[async_trait::async_trait]
pub trait Table: ScannableTable {
    /// Append an entry to the table.
    async fn append(&self, data: DataChunk) -> Result<usize>;

    /// Write a batch of changes. For now, we use `StreamChunk` to represent a write batch
    /// An assertion is put to assert only insertion operations are allowed.
    fn write(&self, chunk: &StreamChunk) -> Result<usize>;

    /// Get the column ids of the table.
    fn get_column_ids(&self) -> Vec<i32>;

    /// Get the indices of the specific column.
    fn index_of_column_id(&self, column_id: i32) -> Result<usize>;
}

#[derive(Clone, Debug)]
pub struct TableColumnDesc {
    pub data_type: DataTypeRef,
    pub column_id: i32,
}

pub enum TableScanOptions {
    SequentialScan,
    SparseIndexScan,
}
