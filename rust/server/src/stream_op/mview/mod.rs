mod mview_sink;
mod mview_state;
mod mview_table;

use bytes::BufMut;

use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::{serialize_datum_into, Datum};

use super::OrderedRowSerializer;

fn serialize_pk(pk: &Row, serializer: &OrderedRowSerializer) -> Result<Vec<u8>> {
    let mut result = vec![];
    serializer.order_based_scehmaed_serialize(pk, &mut result);
    Ok(std::mem::take(&mut result[0]))
}

fn serialize_cell_idx(cell_idx: u32) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(4);
    buf.put_u32_le(cell_idx);
    debug_assert_eq!(buf.len(), 4);
    Ok(buf)
}

fn serialize_cell(cell: &Datum) -> Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::default();
    serialize_datum_into(cell, &mut serializer)?;
    Ok(serializer.into_inner())
}

pub use mview_sink::*;
pub use mview_state::*;
pub use mview_table::*;
