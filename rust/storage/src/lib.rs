#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(unused_must_use)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(drain_filter)]
#![feature(bound_map)]
#![feature(backtrace)]
#![feature(map_first_last)]

use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use table::ScannableTable;

pub mod bummock;
pub mod hummock;
pub mod keyspace;
pub mod memory;
pub mod metrics;
pub mod monitor;
pub mod object;
pub mod panic_store;
mod store;
pub mod table;
pub mod write_batch;

#[cfg(feature = "rocksdb-local")]
pub mod rocksdb_local;
#[cfg(not(feature = "rocksdb-local"))]
#[path = "rocksdb_local_mock.rs"]
pub mod rocksdb_local;

#[cfg(feature = "tikv")]
pub mod tikv;
#[cfg(not(feature = "tikv"))]
#[path = "tikv_mock.rs"]
pub mod tikv;

pub use keyspace::{Keyspace, Segment};
pub use store::{StateStore, StateStoreImpl, StateStoreIter};

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
    pub data_type: DataType,
    pub column_id: i32,
    pub name: String, // for debugging
}

impl TableColumnDesc {
    pub fn new_without_name(column_id: i32, data_type: DataType) -> TableColumnDesc {
        TableColumnDesc {
            data_type,
            column_id,
            name: String::new(),
        }
    }
}

pub enum TableScanOptions {
    SequentialScan,
    SparseIndexScan,
}
