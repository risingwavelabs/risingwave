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

mod top_n_bottom_n_state;
mod top_n_state;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
use risingwave_storage::StateStoreIter;
pub use top_n_bottom_n_state::ManagedTopNBottomNState;
pub use top_n_state::ManagedTopNState;

pub mod variants {
    pub const TOP_N_MIN: usize = 0;
    pub const TOP_N_MAX: usize = 1;
}

fn deserialize_pk<const TOP_N_TYPE: usize>(
    pk_buf: &mut [u8],
    ordered_row_deserializer: &mut OrderedRowDeserializer,
) -> Result<OrderedRow> {
    if TOP_N_TYPE == variants::TOP_N_MAX {
        pk_buf.iter_mut().for_each(|byte| *byte = !*byte);
    }
    // We just encounter the start of a new row, so we finalize the previous one.
    let pk = ordered_row_deserializer.deserialize(pk_buf)?;
    Ok(pk)
}

// The function moves `pk_row_bytes_iter` as it is possible that we would take one extra cell from
// it than the returned result. Allowing it to be reused after this function via borrowing may cause
// confusion and bugs.
async fn deserialize_bytes_to_pk_and_row<
    const TOP_N_TYPE: usize,
    I: StateStoreIter<Item = (Bytes, Bytes)>,
>(
    mut pk_row_bytes_iter: I,
    ordered_row_deserializer: &mut OrderedRowDeserializer,
    cell_based_row_deserializer: &mut CellBasedRowDeserializer,
    num_rows: Option<usize>,
) -> Result<Vec<(OrderedRow, Row)>> {
    let mut result = vec![];
    while let Some((key, value)) = pk_row_bytes_iter.next().await? {
        let pk_buf_and_row = cell_based_row_deserializer.deserialize(&key, &value)?;
        match pk_buf_and_row {
            Some((mut pk_buf, row)) => {
                let pk = deserialize_pk::<TOP_N_TYPE>(&mut pk_buf, ordered_row_deserializer)?;
                result.push((pk, row));
                if let Some(num_rows) = num_rows && result.len() == num_rows {
                    return Ok(result);
                }
            }
            None => {}
        }
    }
    // Reaching here implies that all the key value pairs have been drained from
    // `pk_row_bytes_iter`. Try to take out the final row.
    // It is possible that `iter` is empty, so we may read nothing and there is no such final row.
    let pk_buf_and_row = cell_based_row_deserializer.take();
    if let Some(mut pk_buf_and_row) = pk_buf_and_row {
        let pk = deserialize_pk::<TOP_N_TYPE>(&mut pk_buf_and_row.0, ordered_row_deserializer)?;
        result.push((pk, pk_buf_and_row.1));
    }
    Ok(result)
}
