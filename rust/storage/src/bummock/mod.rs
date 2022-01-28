//! The implementation for bummock columnar storage.

#![allow(dead_code)]

pub use rg::*;
pub use table::*;

mod rg;
mod table;

use risingwave_common::array::DataChunkRef;

pub enum BummockResult {
    Data(Vec<DataChunkRef>),
    DataEof,
}
