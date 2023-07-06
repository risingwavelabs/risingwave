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

//! Column-aware row encoding is an encoding format which converts row into a binary form that
//! remains explanable after schema changes
//! Current design of flag just contains 1 meaningful information: the 2 LSBs represents
//! the size of offsets: `u8`/`u16`/`u32`
//! We have a `Serializer` and a `Deserializer` for each schema of `Row`, which can be reused
//! until schema changes

#![expect(clippy::disallowed_methods, reason = "used by bitflags!")]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bitflags::bitflags;

use super::*;
use crate::catalog::ColumnId;
use crate::row::Row;

// deprecated design of have a Width to represent number of datum
// may be considered should `ColumnId` representation be optimized
// #[derive(Clone, Copy)]
// enum Width {
//     Mid(u8),
//     Large(u16),
//     Extra(u32),
// }

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Flag: u8 {
        const EMPTY = 0b_1000_0000;
        const OFFSET8 = 0b01;
        const OFFSET16 = 0b10;
        const OFFSET32 = 0b11;
    }
}

/// `RowEncoding` holds row-specific information for Column-Aware Encoding
struct RowEncoding {
    flag: Flag,
    offsets: Vec<u8>,
    buf: Vec<u8>,
}

impl RowEncoding {
    fn new() -> Self {
        RowEncoding {
            flag: Flag::EMPTY,
            offsets: vec![],
            buf: vec![],
        }
    }

    fn set_offsets(&mut self, usize_offsets: &[usize], max_offset: usize) {
        debug_assert!(self.offsets.is_empty());
        match max_offset {
            _n @ ..=const { u8::MAX as usize } => {
                self.flag |= Flag::OFFSET8;
                usize_offsets
                    .iter()
                    .for_each(|m| self.offsets.put_u8(*m as u8));
            }
            _n @ ..=const { u16::MAX as usize } => {
                self.flag |= Flag::OFFSET16;
                usize_offsets
                    .iter()
                    .for_each(|m| self.offsets.put_u16_le(*m as u16));
            }
            _n @ ..=const { u32::MAX as usize } => {
                self.flag |= Flag::OFFSET32;
                usize_offsets
                    .iter()
                    .for_each(|m| self.offsets.put_u32_le(*m as u32));
            }
            _ => unreachable!("encoding length exceeds u32"),
        }
    }

    fn encode(&mut self, datum_refs: impl Iterator<Item = impl ToDatumRef>) {
        debug_assert!(
            self.buf.is_empty(),
            "should not encode one RowEncoding object multiple times."
        );
        let mut offset_usize = vec![];
        for datum in datum_refs {
            offset_usize.push(self.buf.len());
            if let Some(v) = datum.to_datum_ref() {
                serialize_scalar(v, &mut self.buf);
            }
        }
        let max_offset = *offset_usize
            .last()
            .expect("should encode at least one column");
        self.set_offsets(&offset_usize, max_offset);
    }
}

/// Column-Aware `Serializer` holds schema related information, and shall be
/// created again once the schema changes
#[derive(Clone)]
pub struct Serializer {
    encoded_column_ids: Vec<u8>,
    datum_num: u32,
}

impl Serializer {
    /// Create a new `Serializer` with current `column_ids`
    pub fn new(column_ids: &[ColumnId]) -> Self {
        // currently we hard-code ColumnId as i32
        let mut encoded_column_ids = Vec::with_capacity(column_ids.len() * 4);
        for id in column_ids {
            encoded_column_ids.put_i32_le(id.get_id());
        }
        let datum_num = column_ids.len() as u32;
        Self {
            encoded_column_ids,
            datum_num,
        }
    }

    fn serialize_inner(&self, encoding: RowEncoding) -> Vec<u8> {
        let mut row_bytes = Vec::with_capacity(
            5 + self.encoded_column_ids.len() + encoding.offsets.len() + encoding.buf.len(), /* 5 comes from u8+u32 */
        );
        row_bytes.put_u8(encoding.flag.bits());
        row_bytes.put_u32_le(self.datum_num);
        row_bytes.extend(&self.encoded_column_ids);
        row_bytes.extend(&encoding.offsets);
        row_bytes.extend(&encoding.buf);

        row_bytes
    }
}

impl ValueRowSerializer for Serializer {
    /// Serialize a row under the schema of the Serializer
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        assert_eq!(row.len(), self.datum_num as usize);
        let mut encoding = RowEncoding::new();
        encoding.encode(row.iter());
        self.serialize_inner(encoding)
    }
}

/// Column-Aware `Deserializer` holds needed `ColumnIds` and their corresponding schema
/// Should non-null default values be specified, a new field could be added to Deserializer
#[derive(Clone)]
pub struct Deserializer {
    needed_column_ids: BTreeMap<i32, usize>,
    schema: Arc<[DataType]>,
    default_column_values: Vec<(usize, Datum)>,
}

impl Deserializer {
    pub fn new(
        column_ids: &[ColumnId],
        schema: Arc<[DataType]>,
        column_with_default: impl Iterator<Item = (usize, Datum)>,
    ) -> Self {
        assert_eq!(column_ids.len(), schema.len());
        Self {
            needed_column_ids: column_ids
                .iter()
                .enumerate()
                .map(|(i, c)| (c.get_id(), i))
                .collect::<BTreeMap<_, _>>(),
            schema,
            default_column_values: column_with_default.collect(),
        }
    }
}

impl ValueRowDeserializer for Deserializer {
    fn deserialize(&self, mut encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        let flag = Flag::from_bits(encoded_bytes.get_u8()).expect("should be a valid flag");
        let offset_bytes = match flag - Flag::EMPTY {
            Flag::OFFSET8 => 1,
            Flag::OFFSET16 => 2,
            Flag::OFFSET32 => 4,
            _ => return Err(ValueEncodingError::InvalidFlag(flag.bits())),
        };
        let datum_num = encoded_bytes.get_u32_le() as usize;
        let offsets_start_idx = 4 * datum_num;
        let data_start_idx = offsets_start_idx + datum_num * offset_bytes;
        let offsets = &encoded_bytes[offsets_start_idx..data_start_idx];
        let data = &encoded_bytes[data_start_idx..];
        let mut datums = vec![None; self.schema.len()];
        let mut contained_indices = BTreeSet::new();
        for i in 0..datum_num {
            let this_id = encoded_bytes.get_i32_le();
            if let Some(&decoded_idx) = self.needed_column_ids.get(&this_id) {
                contained_indices.insert(decoded_idx);
                let this_offset_start_idx = i * offset_bytes;
                let mut this_offset_slice =
                    &offsets[this_offset_start_idx..(this_offset_start_idx + offset_bytes)];
                let this_offset = deserialize_width(offset_bytes, &mut this_offset_slice);
                let data = if i + 1 < datum_num {
                    let mut next_offset_slice = &offsets[(this_offset_start_idx + offset_bytes)
                        ..(this_offset_start_idx + 2 * offset_bytes)];
                    let next_offset = deserialize_width(offset_bytes, &mut next_offset_slice);
                    if this_offset == next_offset {
                        None
                    } else {
                        let mut data_slice = &data[this_offset..next_offset];
                        Some(deserialize_value(
                            &self.schema[decoded_idx],
                            &mut data_slice,
                        )?)
                    }
                } else if this_offset == data.len() {
                    None
                } else {
                    let mut data_slice = &data[this_offset..];
                    Some(deserialize_value(
                        &self.schema[decoded_idx],
                        &mut data_slice,
                    )?)
                };
                datums[decoded_idx] = data;
            }
        }
        for (id, datum) in &self.default_column_values {
            if !contained_indices.contains(id) && datums[*id].is_none() {
                datums[*id] = datum.clone()
            }
        }
        Ok(datums)
    }
}

fn deserialize_width(len: usize, data: &mut impl Buf) -> usize {
    match len {
        1 => data.get_u8() as usize,
        2 => data.get_u16_le() as usize,
        4 => data.get_u32_le() as usize,
        _ => unreachable!("Width's len should be either 1, 2, or 4"),
    }
}

/// Combined column-aware `Serializer` and `Deserializer` given the same
/// `column_ids` and `schema`
#[derive(Clone)]
pub struct ColumnAwareSerde {
    pub serializer: Serializer,
    pub deserializer: Deserializer,
}

impl ValueRowSerializer for ColumnAwareSerde {
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        self.serializer.serialize(row)
    }
}

impl ValueRowDeserializer for ColumnAwareSerde {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        self.deserializer.deserialize(encoded_bytes)
    }
}
