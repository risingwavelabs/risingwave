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
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_storage::cell_based_row_deserializer::GeneralCellBasedRowDeserializer;
use risingwave_storage::StateStoreIter;
pub use top_n_bottom_n_state::ManagedTopNBottomNState;
pub use top_n_state::ManagedTopNState;

use crate::executor::error::{StreamExecutorError, StreamExecutorResult};

pub mod variants {
    pub const TOP_N_MIN: usize = 0;
    pub const TOP_N_MAX: usize = 1;
}

fn deserialize_pk<const TOP_N_TYPE: usize>(
    pk_buf: &mut [u8],
    ordered_row_deserializer: &mut OrderedRowDeserializer,
) -> StreamExecutorResult<OrderedRow> {
    if TOP_N_TYPE == variants::TOP_N_MAX {
        pk_buf.iter_mut().for_each(|byte| *byte = !*byte);
    }
    // We just encounter the start of a new row, so we finalize the previous one.
    let pk = ordered_row_deserializer
        .deserialize(pk_buf)
        .map_err(StreamExecutorError::serde_error)?;
    Ok(pk)
}

pub struct PkAndRowIterator<'a, I: StateStoreIter<Item = (Bytes, Bytes)>, const TOP_N_TYPE: usize> {
    iter: I,
    ordered_row_deserializer: &'a mut OrderedRowDeserializer,
    cell_based_row_deserializer: &'a mut GeneralCellBasedRowDeserializer,
}

impl<'a, I: StateStoreIter<Item = (Bytes, Bytes)>, const TOP_N_TYPE: usize>
    PkAndRowIterator<'a, I, TOP_N_TYPE>
{
    pub fn new(
        iter: I,
        ordered_row_deserializer: &'a mut OrderedRowDeserializer,
        cell_based_row_deserializer: &'a mut GeneralCellBasedRowDeserializer,
    ) -> Self {
        Self {
            iter,
            ordered_row_deserializer,
            cell_based_row_deserializer,
        }
    }

    async fn deserialize_bytes_to_pk_and_row(
        &mut self,
    ) -> StreamExecutorResult<Option<(OrderedRow, Row)>> {
        while let Some((key, value)) = self.iter.next().await? {
            let pk_buf_and_row = self
                .cell_based_row_deserializer
                .deserialize(&key, &value)
                .map_err(StreamExecutorError::serde_error)?;
            match pk_buf_and_row {
                Some((mut pk_buf, row)) => {
                    let pk =
                        deserialize_pk::<TOP_N_TYPE>(&mut pk_buf, self.ordered_row_deserializer)
                            .map_err(StreamExecutorError::serde_error)?;
                    return Ok(Some((pk, row)));
                }
                None => {}
            }
        }
        // Reaching here implies that all the key value pairs have been drained from `self.iter`.
        // Try to take out the final row.
        // It is possible that `self.iter` is empty, so we may read nothing and there is no such
        // final row.
        let pk_buf_and_row = self.cell_based_row_deserializer.take();
        if let Some(mut pk_buf_and_row) = pk_buf_and_row {
            let pk =
                deserialize_pk::<TOP_N_TYPE>(&mut pk_buf_and_row.0, self.ordered_row_deserializer)
                    .map_err(StreamExecutorError::serde_error)?;
            Ok(Some((pk, pk_buf_and_row.1)))
        } else {
            Ok(None)
        }
    }

    pub async fn next(&mut self) -> StreamExecutorResult<Option<(OrderedRow, Row)>> {
        let pk_and_row = self.deserialize_bytes_to_pk_and_row().await?;
        Ok(pk_and_row)
    }
}

impl<'a, I: StateStoreIter<Item = (Bytes, Bytes)>, const TOP_N_TYPE: usize> Drop
    for PkAndRowIterator<'a, I, TOP_N_TYPE>
{
    fn drop(&mut self) {
        self.cell_based_row_deserializer.reset();
    }
}
