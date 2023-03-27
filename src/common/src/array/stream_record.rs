// Copyright 2023 RisingWave Labs
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

use auto_enums::auto_enum;

use super::StreamChunk;
use crate::array::Op;
use crate::row::Row;
use crate::types::DataType;

/// Type of a row change, without row data.
pub enum RecordType {
    Insert,
    Delete,
    Update,
}

/// Generic type to represent a row change.
#[derive(Debug, Clone)]
pub enum Record<R: Row> {
    Insert { new_row: R },
    Delete { old_row: R },
    Update { old_row: R, new_row: R },
}

impl<R: Row> Record<R> {
    /// Convert this stream record to one or two rows with corresponding ops.
    #[auto_enum(Iterator)]
    pub fn into_rows(self) -> impl Iterator<Item = (Op, R)> {
        match self {
            Record::Insert { new_row } => std::iter::once((Op::Insert, new_row)),
            Record::Delete { old_row } => std::iter::once((Op::Delete, old_row)),
            Record::Update { old_row, new_row } => {
                [(Op::UpdateDelete, old_row), (Op::UpdateInsert, new_row)].into_iter()
            }
        }
    }

    /// Get record type of this record.
    pub fn to_record_type(&self) -> RecordType {
        match self {
            Record::Insert { .. } => RecordType::Insert,
            Record::Delete { .. } => RecordType::Delete,
            Record::Update { .. } => RecordType::Update,
        }
    }

    /// Convert this stream record to a stream chunk containing only 1 or 2 rows.
    pub fn to_stream_chunk(&self, data_types: &[DataType]) -> StreamChunk {
        match self {
            Record::Insert { new_row } => {
                StreamChunk::from_rows(&[(Op::Insert, new_row.to_owned_row())], data_types)
            }
            Record::Delete { old_row } => {
                StreamChunk::from_rows(&[(Op::Delete, old_row.to_owned_row())], data_types)
            }
            Record::Update { old_row, new_row } => StreamChunk::from_rows(
                &[
                    (Op::Delete, old_row.to_owned_row()),
                    (Op::Insert, new_row.to_owned_row()),
                ],
                data_types,
            ),
        }
    }
}
