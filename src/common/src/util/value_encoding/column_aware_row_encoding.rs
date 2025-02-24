// Copyright 2025 RisingWave Labs
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

use std::collections::HashSet;
use std::sync::Arc;

use ahash::HashMap;
use bitfield_struct::bitfield;

use super::*;
use crate::catalog::ColumnId;

/// The width of the offset of the encoded data, i.e., how many bytes are used to represent the offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum OffsetWidth {
    Unset = 0b00,
    /// The offset of encoded data can be represented by u8.
    Offset8 = 0b01,
    /// The offset of encoded data can be represented by u16.
    Offset16 = 0b10,
    /// The offset of encoded data can be represented by u32.
    Offset32 = 0b11,
}

impl OffsetWidth {
    /// Get the width of the offset in bytes.
    const fn width(self) -> Option<usize> {
        Some(match self {
            OffsetWidth::Unset => return None,
            OffsetWidth::Offset8 => 1,
            OffsetWidth::Offset16 => 2,
            OffsetWidth::Offset32 => 4,
        })
    }

    const fn into_bits(self) -> u8 {
        self as u8
    }

    const fn from_bits(bits: u8) -> Self {
        match bits {
            0b00 => OffsetWidth::Unset,
            0b01 => OffsetWidth::Offset8,
            0b10 => OffsetWidth::Offset16,
            0b11 => OffsetWidth::Offset32,
            _ => unreachable!(),
        }
    }
}

#[bitfield(u8, order = Msb)]
#[derive(PartialEq, Eq)]
struct Header {
    /// Magic bit to indicate it's column-aware encoding.
    /// Note that in plain value encoding, the first byte is always 0 or 1 for nullability,
    /// of which the most significant bit is always 0.
    #[bits(1, default = true, access = RO)]
    magic: bool,

    #[bits(5)]
    _reserved: u8,

    /// Indicate the offset width of the encoded data.
    #[bits(2)]
    offset: OffsetWidth,
}

/// `RowEncoding` holds row-specific information for Column-Aware Encoding
struct RowEncoding {
    header: Header,
    offsets: Vec<u8>,
    buf: Vec<u8>,
}

impl RowEncoding {
    fn new() -> Self {
        RowEncoding {
            header: Header::new(),
            offsets: vec![],
            buf: vec![],
        }
    }

    fn set_offsets(&mut self, usize_offsets: &[usize]) {
        debug_assert!(
            self.offsets.is_empty(),
            "should not set offsets multiple times"
        );

        // Use 0 if no data is present.
        let max_offset = usize_offsets.last().copied().unwrap_or(0);

        if max_offset <= u8::MAX as usize {
            self.header.set_offset(OffsetWidth::Offset8);
            usize_offsets
                .iter()
                .for_each(|m| self.offsets.put_u8(*m as u8));
        } else if max_offset <= u16::MAX as usize {
            self.header.set_offset(OffsetWidth::Offset16);
            usize_offsets
                .iter()
                .for_each(|m| self.offsets.put_u16_le(*m as u16));
        } else if max_offset <= u32::MAX as usize {
            self.header.set_offset(OffsetWidth::Offset32);
            usize_offsets
                .iter()
                .for_each(|m| self.offsets.put_u32_le(*m as u32));
        } else {
            panic!("encoding length {} exceeds u32", max_offset);
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
        self.set_offsets(&offset_usize);
    }

    // TODO: Avoid duplicated code. `encode_slice` is the same as `encode` except it doesn't require column type.
    fn encode_slice<'a>(&mut self, datum_refs: impl Iterator<Item = Option<&'a [u8]>>) {
        debug_assert!(
            self.buf.is_empty(),
            "should not encode one RowEncoding object multiple times."
        );
        let mut offset_usize = vec![];
        for datum in datum_refs {
            offset_usize.push(self.buf.len());
            if let Some(v) = datum {
                self.buf.put_slice(v);
            }
        }
        self.set_offsets(&offset_usize);
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
        row_bytes.put_u8(encoding.header.into_bits());
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
    required_column_ids: HashMap<i32, usize>,
    schema: Arc<[DataType]>,

    /// A row with default values for each column or `None` if no default value is specified
    default_row: Vec<Datum>,
}

impl Deserializer {
    pub fn new(
        column_ids: &[ColumnId],
        schema: Arc<[DataType]>,
        column_with_default: impl Iterator<Item = (usize, Datum)>,
    ) -> Self {
        assert_eq!(column_ids.len(), schema.len());
        let mut default_row: Vec<Datum> = vec![None; schema.len()];
        for (i, datum) in column_with_default {
            default_row[i] = datum;
        }
        Self {
            required_column_ids: column_ids
                .iter()
                .enumerate()
                .map(|(i, c)| (c.get_id(), i))
                .collect::<HashMap<_, _>>(),
            schema,
            default_row,
        }
    }
}

impl ValueRowDeserializer for Deserializer {
    fn deserialize(&self, mut encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        let header_bits = encoded_bytes.get_u8();
        let header = Header::from_bits(header_bits);
        if !header.magic() {
            return Err(ValueEncodingError::InvalidFlag(header_bits));
        }
        let offset_bytes =
            (header.offset().width()).ok_or(ValueEncodingError::InvalidFlag(header_bits))?;

        let datum_num = encoded_bytes.get_u32_le() as usize;
        let offsets_start_idx = 4 * datum_num;
        let data_start_idx = offsets_start_idx + datum_num * offset_bytes;
        let offsets = &encoded_bytes[offsets_start_idx..data_start_idx];
        let data = &encoded_bytes[data_start_idx..];

        let mut row = self.default_row.clone();
        for i in 0..datum_num {
            let this_id = encoded_bytes.get_i32_le();
            if let Some(&decoded_idx) = self.required_column_ids.get(&this_id) {
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
                row[decoded_idx] = data;
            }
        }
        Ok(row)
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

/// Deserializes row `encoded_bytes`, drops columns not in `valid_column_ids`, serializes and returns.
/// If no column is dropped, returns None.
// TODO: Avoid duplicated code. The current code combines`Serializer` and `Deserializer` with unavailable parameter removed, e.g. `Deserializer::schema`.
pub fn try_drop_invalid_columns(
    mut encoded_bytes: &[u8],
    valid_column_ids: &HashSet<i32>,
) -> Option<Vec<u8>> {
    let header = Header::from_bits(encoded_bytes.get_u8());
    if !header.magic() {
        // invalid flag, do not proceed
        return None;
    }

    let datum_num = encoded_bytes.get_u32_le() as usize;
    let mut is_column_dropped = false;
    let mut encoded_bytes_copy = encoded_bytes;
    for _ in 0..datum_num {
        let this_id = encoded_bytes_copy.get_i32_le();
        if !valid_column_ids.contains(&this_id) {
            is_column_dropped = true;
            break;
        }
    }
    if !is_column_dropped {
        return None;
    }

    // Slow path that drops columns. Should be rare.
    let offset_bytes = header.offset().width().expect("offset width is unset");
    let offsets_start_idx = 4 * datum_num;
    let data_start_idx = offsets_start_idx + datum_num * offset_bytes;
    let offsets = &encoded_bytes[offsets_start_idx..data_start_idx];
    let data = &encoded_bytes[data_start_idx..];
    let mut datums: Vec<Option<&[u8]>> = Vec::with_capacity(valid_column_ids.len());
    let mut column_ids = Vec::with_capacity(valid_column_ids.len());
    for i in 0..datum_num {
        let this_id = encoded_bytes.get_i32_le();
        if valid_column_ids.contains(&this_id) {
            column_ids.push(this_id);
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
                    let data_slice = &data[this_offset..next_offset];
                    Some(data_slice)
                }
            } else if this_offset == data.len() {
                None
            } else {
                let data_slice = &data[this_offset..];
                Some(data_slice)
            };
            datums.push(data);
        }
    }
    if column_ids.is_empty() {
        // According to `RowEncoding::encode`, at least one column is required.
        return None;
    }

    let mut encoding = RowEncoding::new();
    encoding.encode_slice(datums.into_iter());
    let mut encoded_column_ids = Vec::with_capacity(column_ids.len() * 4);
    let datum_num = column_ids.len() as u32;
    for id in column_ids {
        encoded_column_ids.put_i32_le(id);
    }
    let mut row_bytes = Vec::with_capacity(
        5 + encoded_column_ids.len() + encoding.offsets.len() + encoding.buf.len(), /* 5 comes from u8+u32 */
    );
    row_bytes.put_u8(encoding.header.into_bits());
    row_bytes.put_u32_le(datum_num);
    row_bytes.extend(&encoded_column_ids);
    row_bytes.extend(&encoding.offsets);
    row_bytes.extend(&encoding.buf);

    Some(row_bytes)
}
