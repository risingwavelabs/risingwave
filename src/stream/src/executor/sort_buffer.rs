// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use gen_iter::GenIter;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{ScalarImpl, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_storage::StateStore;

use super::{PkIndices, Watermark};
use crate::common::table::state_table::StateTable;

/// [`SortBufferKey`] contains a record's timestamp and pk.
type SortBufferKey = (ScalarImpl, OwnedRow);

/// [`SortBufferValue`] contains a record's value and a flag indicating whether the record has been
/// persisted to storage.
/// NOTE: There is an exhausting trade-off for which structure to use for the in-memory buffer. For
/// example, up to 8x memory can be used with [`OwnedRow`] compared to the `CompactRow`. However, if
/// there are only a few rows that will be temporarily stored in the buffer during an epoch,
/// [`OwnedRow`] will be more efficient instead due to no ser/de needed. So here we could do further
/// optimizations.
type SortBufferValue = (OwnedRow, bool);

/// [`SortBuffer`] is a common component that consume an unordered stream and produce an ordered
/// stream by watermark. This component maintains a state table internally, which schema is same as
/// its input and output. Generally, the component acts as a buffer that output the data it received
/// with a delay.
pub struct SortBuffer<S: StateStore> {
    schema: Schema,

    pk_indices: PkIndices,

    sort_column_index: usize,

    chunk_size: usize,

    state_table: StateTable<S>,

    buffer: BTreeMap<SortBufferKey, SortBufferValue>,
}

impl<S: StateStore> SortBuffer<S> {
    /// Store all rows in one [`StreamChunk`] to the buffer.
    /// TODO: Only insertions are supported now.
    pub fn handle_chunk(&mut self, chunk: &StreamChunk) {
        for (op, row_ref) in chunk.rows() {
            assert_eq!(
                op,
                Op::Insert,
                "operations other than insert currently are not supported by sort executor"
            );
            let timestamp_datum = row_ref
                .datum_at(self.sort_column_index)
                .to_owned_datum()
                .unwrap();
            let pk = row_ref.project(&self.pk_indices).into_owned_row();
            let row = row_ref.into_owned_row();
            self.buffer.insert((timestamp_datum, pk), (row, false));
        }
    }

    /// Handle the watermark and output a stream that is ordered by the sort key.
    pub fn handle_watermark(
        &mut self,
        watermark: Watermark,
    ) -> impl Iterator<Item = DataChunk> + '_ {
        let g = move || {
            let Watermark {
                col_idx,
                data_type: _,
                val,
            } = watermark;
            if col_idx != self.sort_column_index {
                return;
            }
            let mut data_chunk_builder =
                DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
            let watermark_value = val;
            // Find out the records to send to downstream.
            for (key, (row, _)) in &self.buffer {
                // Only when a record's timestamp is prior to the watermark should it be
                // sent to downstream.
                if key.0 < watermark_value {
                    // Add the record to stream chunk data. Note that we retrieve the
                    // record from a BTreeMap, so data in this chunk should be ordered
                    // by timestamp and pk.
                    if let Some(data_chunk) = data_chunk_builder.append_one_row(row) {
                        // When the chunk size reaches its maximum, we construct a data chunk and
                        // send it to downstream.
                        yield data_chunk;
                    }
                } else {
                    // We have collected all data below watermark.
                    break;
                }
            }

            // Construct and send a data chunk message. Rows in this message are
            // always ordered by timestamp.
            if let Some(data_chunk) = data_chunk_builder.consume_all() {
                yield data_chunk;
            }
        };
        GenIter::from(g)
    }

    /// Delete a record from the buffer.
    /// TODO: The delete is always be called in order per group, so should we support range_delete
    /// here?
    ///
    /// # Panics
    ///
    /// Panics if the record is not found in the buffer.
    pub fn delete(&mut self, row: impl Row) {
        let timestamp_datum = row
            .datum_at(self.sort_column_index)
            .to_owned_datum()
            .unwrap();
        let pk = row.project(&self.pk_indices).into_owned_row();
        let (_, (row, persisted)) = self.buffer.remove_entry(&(timestamp_datum, pk)).unwrap();
        if persisted {
            self.state_table.delete(&row);
        }
    }
}
