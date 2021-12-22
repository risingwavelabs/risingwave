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
pub mod keyspace;
pub mod memory;
pub mod object;
pub mod panic_store;
pub mod table;

use async_trait::async_trait;
use bytes::Bytes;
pub use keyspace::{Keyspace, Segment};
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
pub trait StateStoreIter: Send + 'static {
    type Item;

    async fn open(&mut self) -> Result<()>;

    async fn next(&mut self) -> Result<Option<Self::Item>>;
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
