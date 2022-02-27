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
#![feature(let_chains)]

use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

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
use risingwave_common::catalog::ColumnId;
pub use store::{StateStore, StateStoreImpl, StateStoreIter};

#[derive(Clone, Debug)]
pub struct TableColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String, // for debugging
}

#[derive(Clone, Debug)]
pub struct IndexDesc {
    pub column_id: ColumnId,
    pub data_type: DataType,
    pub order: OrderType,
}

impl TableColumnDesc {
    pub fn unnamed(column_id: ColumnId, data_type: DataType) -> TableColumnDesc {
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
