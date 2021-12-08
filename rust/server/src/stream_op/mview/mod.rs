mod mview_sink;
mod mview_state;
mod mview_table;

use risingwave_common::array::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{
    serialize_datum_into, serialize_datum_not_null_into, Datum, Scalar,
};

fn serialize_pk(pk: &Row) -> Result<Vec<u8>> {
    let mut pk_serializer = memcomparable::Serializer::default();
    for datum in &pk.0 {
        serialize_datum_into(datum, &mut pk_serializer).map_err(ErrorCode::MemComparableError)?;
    }
    Ok(pk_serializer.into_inner())
}

fn serialize_cell_idx(cell_idx: i32) -> Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::default();
    serialize_datum_not_null_into(&Some(cell_idx.to_scalar_value()), &mut serializer)
        .map_err(ErrorCode::MemComparableError)?;
    Ok(serializer.into_inner())
}

fn serialize_cell(cell: &Datum) -> Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::default();
    serialize_datum_into(cell, &mut serializer).map_err(ErrorCode::MemComparableError)?;
    Ok(serializer.into_inner())
}

pub use mview_sink::*;
pub use mview_state::*;
pub use mview_table::*;
