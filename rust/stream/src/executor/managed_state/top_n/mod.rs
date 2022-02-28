mod top_n_bottom_n_state;
mod top_n_state;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
pub use top_n_bottom_n_state::ManagedTopNBottomNState;
pub use top_n_state::ManagedTopNState;

pub mod variants {
    pub const TOP_N_MIN: usize = 0;
    pub const TOP_N_MAX: usize = 1;
}

fn deserialize_inner<const TOP_N_TYPE: usize>(
    pk_buf: &mut [u8],
    ordered_row_deserializer: &mut OrderedRowDeserializer,
    cell_based_row_deserializer: &mut CellBasedRowDeserializer,
) -> Result<(OrderedRow, Row)> {
    if TOP_N_TYPE == variants::TOP_N_MAX {
        pk_buf.iter_mut().for_each(|byte| *byte = !*byte);
    }
    // We just encounter the start of a new row, so we finalize the previous one.
    let pk = ordered_row_deserializer.deserialize(pk_buf)?;
    // `None` should NOT be returned
    let row = cell_based_row_deserializer
        .take()
        .ok_or_else(|| ErrorCode::InternalError("Invalid encoding for Row".to_string()))?;
    Ok((pk, row))
}

fn deserialize_bytes_to_pk_and_row<const TOP_N_TYPE: usize>(
    pk_row_bytes: Vec<(Bytes, Bytes)>,
    ordered_row_deserializer: &mut OrderedRowDeserializer,
    cell_based_row_deserializer: &mut CellBasedRowDeserializer,
) -> Result<Vec<(OrderedRow, Row)>> {
    if pk_row_bytes.is_empty() {
        return Ok(vec![]);
    }
    let mut result = vec![];
    // We initialize the `pk_buf` to be the first element so that we don't need to put a check
    // inside the loop to specially check the first corner case.
    let first_key = &pk_row_bytes[0].0;
    let mut pk_buf = first_key[0..first_key.len() - 4].to_vec();
    for (key, value) in pk_row_bytes {
        let cur_pk_buf = &key[0..key.len() - 4];
        if pk_buf != cur_pk_buf {
            let ret = deserialize_inner::<TOP_N_TYPE>(
                &mut pk_buf,
                ordered_row_deserializer,
                cell_based_row_deserializer,
            )?;
            result.push(ret);
            pk_buf = cur_pk_buf.to_vec();
        }
        cell_based_row_deserializer.deserialize(&key, &value)?;
    }
    // Take out the final row.
    let ret = deserialize_inner::<TOP_N_TYPE>(
        &mut pk_buf,
        ordered_row_deserializer,
        cell_based_row_deserializer,
    )?;
    result.push(ret);
    Ok(result)
}
