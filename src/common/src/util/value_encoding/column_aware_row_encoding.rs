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
use bytes::{Buf, BufMut};
use rw_iter_util::ZipEqDebug;

use super::error::ValueEncodingError;
use super::{Result, ValueRowDeserializer, ValueRowSerializer};
use crate::catalog::ColumnId;
use crate::row::Row;
use crate::types::{DataType, Datum, ScalarRefImpl, ToDatumRef};

mod new_serde {
    use itertools::Itertools;

    use super::*;
    use crate::array::{ListRef, ListValue, MapRef, MapValue, StructRef, StructValue};
    use crate::types::{MapType, ScalarImpl, StructType};
    use crate::util::value_encoding::{
        deserialize_value as plain_deserialize_scalar, serialize_scalar as plain_serialize_scalar,
    };

    fn new_serialize_datum(
        data_type: &DataType,
        datum_ref: impl ToDatumRef,
        buf: &mut impl BufMut,
    ) {
        if let Some(d) = datum_ref.to_datum_ref() {
            buf.put_u8(1);
            new_serialize_scalar(data_type, d, buf)
        } else {
            buf.put_u8(0);
        }
    }

    fn new_serialize_struct(struct_type: &StructType, value: StructRef<'_>, buf: &mut impl BufMut) {
        let serializer = super::Serializer::new_new(
            &[],                          // TODO: column ids
            struct_type.types().cloned(), // TODO: avoid this clone
        );

        let bytes = serializer.serialize(value); // TODO: serialize into the buf directly
        buf.put_u32_le(bytes.len() as _);
        buf.put_slice(&bytes);
    }

    fn new_serialize_list(inner_type: &DataType, value: ListRef<'_>, buf: &mut impl BufMut) {
        let elems = value.iter();
        buf.put_u32_le(elems.len() as u32);

        elems.for_each(|field_value| {
            new_serialize_datum(inner_type, field_value, buf);
        });
    }

    // We don't reuse the serialization of `struct<k, v>[]` for map, as it introduces unnecessary
    // overhead for column ids of `struct<k, v>`, which cannot accommodate schema changes.
    fn new_serialize_map(map_type: &MapType, value: MapRef<'_>, buf: &mut impl BufMut) {
        let elems = value.iter();
        buf.put_u32_le(elems.len() as u32);

        elems.for_each(|(k, v)| {
            new_serialize_scalar(map_type.key(), k, buf);
            new_serialize_datum(map_type.value(), v, buf);
        });
    }

    pub fn new_serialize_scalar(
        data_type: &DataType,
        value: ScalarRefImpl<'_>,
        buf: &mut impl BufMut,
    ) {
        match value {
            ScalarRefImpl::Struct(s) => new_serialize_struct(data_type.as_struct(), s, buf),
            ScalarRefImpl::List(l) => new_serialize_list(data_type.as_list(), l, buf),
            ScalarRefImpl::Map(m) => new_serialize_map(data_type.as_map(), m, buf),

            _ => plain_serialize_scalar(value, buf),
        }
    }

    // --- deserialize ---

    fn new_inner_deserialize_datum(data: &mut impl Buf, ty: &DataType) -> Result<Datum> {
        let null_tag = data.get_u8();
        match null_tag {
            0 => Ok(None),
            1 => Some(new_deserialize_scalar(ty, data)).transpose(),
            _ => Err(ValueEncodingError::InvalidTagEncoding(null_tag)),
        }
    }

    fn new_deserialize_struct(struct_def: &StructType, data: &mut impl Buf) -> Result<ScalarImpl> {
        let deserializer = super::Deserializer::new(
            &[],                                              // TODO: column ids
            struct_def.types().cloned().collect_vec().into(), // TODO: avoid this clone
            std::iter::empty(),
        );
        let encoded_len = data.get_u32_le() as usize;
        let data = data.copy_to_bytes(encoded_len); // TODO: avoid copy
        let fields = deserializer.deserialize(&data)?;

        Ok(ScalarImpl::Struct(StructValue::new(fields)))
    }

    fn new_deserialize_list(item_type: &DataType, data: &mut impl Buf) -> Result<ScalarImpl> {
        let len = data.get_u32_le();
        let mut builder = item_type.create_array_builder(len as usize);
        for _ in 0..len {
            builder.append(new_inner_deserialize_datum(data, item_type)?);
        }
        Ok(ScalarImpl::List(ListValue::new(builder.finish())))
    }

    fn new_deserialize_map(map_type: &MapType, data: &mut impl Buf) -> Result<ScalarImpl> {
        let len = data.get_u32_le();
        let mut builder = map_type
            .clone() // FIXME: clone type everytime here is inefficient
            .into_struct()
            .create_array_builder(len as usize);
        for _ in 0..len {
            let key = new_deserialize_scalar(map_type.key(), data)?;
            let value = new_inner_deserialize_datum(data, map_type.value())?;
            let entry = StructValue::new(vec![Some(key), value]);
            builder.append(Some(ScalarImpl::Struct(entry)));
        }
        Ok(ScalarImpl::Map(MapValue::from_entries(ListValue::new(
            builder.finish(),
        ))))
    }

    pub fn new_deserialize_scalar(ty: &DataType, data: &mut impl Buf) -> Result<ScalarImpl> {
        Ok(match ty {
            DataType::Struct(struct_def) => new_deserialize_struct(struct_def, data)?,
            DataType::List(item_type) => new_deserialize_list(item_type, data)?,
            DataType::Map(map_type) => new_deserialize_map(map_type, data)?,

            _ => plain_deserialize_scalar(ty, data)?,
        })
    }
}

/// The width of the offset of the encoded data, i.e., how many bytes are used to represent the offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum OffsetWidth {
    /// The offset of encoded data can be represented by u8.
    Offset8 = 0b01,
    /// The offset of encoded data can be represented by u16.
    Offset16 = 0b10,
    /// The offset of encoded data can be represented by u32.
    Offset32 = 0b11,
}

impl OffsetWidth {
    /// Get the width of the offset in bytes.
    const fn width(self) -> usize {
        match self {
            OffsetWidth::Offset8 => 1,
            OffsetWidth::Offset16 => 2,
            OffsetWidth::Offset32 => 4,
        }
    }

    const fn into_bits(self) -> u8 {
        self as u8
    }

    const fn from_bits(bits: u8) -> Self {
        match bits {
            0b01 => OffsetWidth::Offset8,
            0b10 => OffsetWidth::Offset16,
            0b11 => OffsetWidth::Offset32,
            _ => panic!("invalid offset width bits"),
        }
    }
}

/// Header (metadata) of the encoded row.
///
/// Layout (most to least significant bits):
///
/// ```text
/// | magic | reserved | offset |
/// |   1   |    5     |   2    |
/// ```
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
    #[bits(2, default = OffsetWidth::Offset8)]
    offset: OffsetWidth,
}

/// `RowEncoding` holds row-specific information for Column-Aware Encoding
struct RowEncoding {
    header: Header,
    offsets: Vec<u8>,
    buf: Vec<u8>,
}

/// A trait unifying [`ToDatumRef`] and already encoded bytes.
trait Encode {
    fn encode_to(self, data_type: &DataType, data: &mut Vec<u8>);
}

impl<T> Encode for T
where
    T: ToDatumRef,
{
    fn encode_to(self, data_type: &DataType, data: &mut Vec<u8>) {
        if let Some(v) = self.to_datum_ref() {
            new_serde::new_serialize_scalar(data_type, v, data);
        }
    }
}

impl Encode for Option<&[u8]> {
    fn encode_to(self, _data_type: &DataType, data: &mut Vec<u8>) {
        if let Some(v) = self {
            data.extend(v);
        }
    }
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

        // Use 0 if there's no data.
        let max_offset = usize_offsets.last().copied().unwrap_or(0);

        let offset_width = match max_offset {
            _n @ ..=const { u8::MAX as usize } => OffsetWidth::Offset8,
            _n @ ..=const { u16::MAX as usize } => OffsetWidth::Offset16,
            _n @ ..=const { u32::MAX as usize } => OffsetWidth::Offset32,
            _ => panic!("encoding length {} exceeds u32", max_offset),
        };
        self.header.set_offset(offset_width);

        self.offsets
            .reserve_exact(usize_offsets.len() * offset_width.width());
        for &offset in usize_offsets {
            self.offsets
                .put_uint_le(offset as u64, offset_width.width());
        }
    }

    fn encode<T: Encode>(&mut self, datums: impl IntoIterator<Item = T>, data_types: &[DataType]) {
        debug_assert!(
            self.buf.is_empty(),
            "should not encode one RowEncoding object multiple times."
        );
        let datums = datums.into_iter();
        let mut offset_usize = Vec::with_capacity(datums.size_hint().0);
        for (datum, data_type) in datums.zip_eq_debug(data_types) {
            offset_usize.push(self.buf.len());
            datum.encode_to(data_type, &mut self.buf);
        }
        self.set_offsets(&offset_usize);
    }
}

/// Column-Aware `Serializer` holds schema related information, and shall be
/// created again once the schema changes
#[derive(Clone)]
pub struct Serializer {
    encoded_column_ids: Vec<u8>,
    data_types: Vec<DataType>,
}

impl Serializer {
    /// Create a new `Serializer` with current `column_ids`
    pub fn new(column_ids: &[ColumnId]) -> Self {
        // currently we hard-code ColumnId as i32
        let mut encoded_column_ids = Vec::with_capacity(column_ids.len() * 4);
        for id in column_ids {
            encoded_column_ids.put_i32_le(id.get_id());
        }

        Self {
            encoded_column_ids,
            data_types: Vec::new(),
        }
    }

    pub fn new_new(column_ids: &[ColumnId], data_types: impl Iterator<Item = DataType>) -> Self {
        // currently we hard-code ColumnId as i32
        let mut encoded_column_ids = Vec::with_capacity(column_ids.len() * 4);
        for id in column_ids {
            encoded_column_ids.put_i32_le(id.get_id());
        }

        Self {
            encoded_column_ids,
            data_types: data_types.collect(),
        }
    }

    fn datum_num(&self) -> usize {
        self.encoded_column_ids.len() / 4
    }

    fn serialize_raw<T: Encode>(&self, datums: impl IntoIterator<Item = T>) -> Vec<u8> {
        let mut encoding = RowEncoding::new();
        encoding.encode(datums, &self.data_types);
        self.finish(encoding)
    }

    fn finish(&self, encoding: RowEncoding) -> Vec<u8> {
        let mut row_bytes = Vec::with_capacity(
            5 + self.encoded_column_ids.len() + encoding.offsets.len() + encoding.buf.len(), /* 5 comes from u8+u32 */
        );
        row_bytes.put_u8(encoding.header.into_bits());
        row_bytes.put_u32_le(self.datum_num() as u32);
        row_bytes.extend(&self.encoded_column_ids);
        row_bytes.extend(&encoding.offsets);
        row_bytes.extend(&encoding.buf);

        row_bytes
    }
}

impl ValueRowSerializer for Serializer {
    /// Serialize a row under the schema of the Serializer
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        assert_eq!(row.len(), self.datum_num());
        self.serialize_raw(row.iter())
    }
}

/// A view of the encoded bytes, which can be iterated over to get the column id and data.
/// Used for deserialization.
// TODO: can we unify this with `RowEncoding`, which is for serialization?
#[derive(Clone)]
struct EncodedBytes<'a> {
    header: Header,

    // When iterating, we will consume `column_ids` and `offsets` while keep `data` unchanged.
    // This is because we record absolute values in `offsets` to index into `data`.
    column_ids: &'a [u8],
    offsets: &'a [u8],
    data: &'a [u8],
}

impl<'a> EncodedBytes<'a> {
    fn new(mut encoded_bytes: &'a [u8]) -> Result<Self> {
        let header = Header::from_bits(encoded_bytes.get_u8());
        if !header.magic() {
            return Err(ValueEncodingError::InvalidFlag(header.into_bits()));
        }
        let offset_bytes = header.offset().width();

        let datum_num = encoded_bytes.get_u32_le() as usize;
        let offsets_start_idx = 4 * datum_num;
        let data_start_idx = offsets_start_idx + datum_num * offset_bytes;

        Ok(EncodedBytes {
            header,
            column_ids: &encoded_bytes[..offsets_start_idx],
            offsets: &encoded_bytes[offsets_start_idx..data_start_idx],
            data: &encoded_bytes[data_start_idx..],
        })
    }
}

impl<'a> Iterator for EncodedBytes<'a> {
    type Item = (i32, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.column_ids.is_empty() {
            assert!(self.offsets.is_empty());
            return None;
        }

        let id = self.column_ids.get_i32_le();

        let offset_width = self.header.offset().width();
        let get_offset = |offsets: &mut &[u8]| offsets.get_uint_le(offset_width) as usize;

        let this_offset = get_offset(&mut self.offsets);
        let next_offset = if self.offsets.is_empty() {
            self.data.len()
        } else {
            let mut peek_offsets = self.offsets; // copy the reference to the slice to avoid mutating the buf position
            get_offset(&mut peek_offsets)
        };

        let data = &self.data[this_offset..next_offset];

        Some((id, data))
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
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        let encoded_bytes = EncodedBytes::new(encoded_bytes)?;

        let mut row = self.default_row.clone();

        for (id, mut data) in encoded_bytes {
            let Some(&decoded_idx) = self.required_column_ids.get(&id) else {
                continue;
            };

            let datum = if data.is_empty() {
                None
            } else {
                Some(new_serde::new_deserialize_scalar(
                    &self.schema[decoded_idx],
                    &mut data,
                )?)
            };

            row[decoded_idx] = datum;
        }

        Ok(row)
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
pub fn try_drop_invalid_columns(
    encoded_bytes: &[u8],
    valid_column_ids: &HashSet<i32>,
) -> Option<Vec<u8>> {
    let encoded_bytes = EncodedBytes::new(encoded_bytes).ok()?;

    let has_invalid_column = (encoded_bytes.clone()).any(|(id, _)| !valid_column_ids.contains(&id));
    if !has_invalid_column {
        return None;
    }

    // Slow path that drops columns. Should be rare.

    let mut datums = Vec::with_capacity(valid_column_ids.len());
    let mut column_ids = Vec::with_capacity(valid_column_ids.len());

    for (id, data) in encoded_bytes {
        if valid_column_ids.contains(&id) {
            column_ids.push(ColumnId::new(id));
            datums.push(if data.is_empty() { None } else { Some(data) });
        }
    }

    let row_bytes = Serializer::new(&column_ids).serialize_raw(datums);
    Some(row_bytes)
}
