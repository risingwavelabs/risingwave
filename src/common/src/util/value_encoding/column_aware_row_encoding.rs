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
use smallvec::{SmallVec, smallvec};

use super::error::ValueEncodingError;
use super::{Result, ValueRowDeserializer, ValueRowSerializer};
use crate::catalog::ColumnId;
use crate::row::Row;
use crate::types::{DataType, Datum, ScalarRefImpl, StructType, ToDatumRef};
use crate::util::value_encoding as plain;

/// Serialize and deserialize functions that recursively use [`ColumnAwareSerde`] for nested fields
/// of composite types.
///
/// Only for column with types that can be altered ([`DataType::can_alter`]).
mod new_serde {
    use super::*;
    use crate::array::{ListRef, ListValue, MapRef, MapValue, StructRef, StructValue};
    use crate::types::{MapType, ScalarImpl, StructType, data_types};

    // --- serialize ---

    // Copy of `plain` but call `new_` for the scalar.
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

    // Different logic from `plain`:
    //
    // Recursively construct a new column-aware `Serializer` for nested fields.
    fn new_serialize_struct(struct_type: &StructType, value: StructRef<'_>, buf: &mut impl BufMut) {
        let serializer = super::Serializer::from_struct(struct_type.clone()); // cloning `StructType` is lightweight

        // `StructRef` is interpreted as a `Row` here.
        let bytes = serializer.serialize(value); // TODO: serialize into the buf directly if we can reserve space accurately
        buf.put_u32_le(bytes.len() as _);
        buf.put_slice(&bytes);
    }

    // Copy of `plain` but call `new_` for the elements.
    fn new_serialize_list(inner_type: &DataType, value: ListRef<'_>, buf: &mut impl BufMut) {
        let elems = value.iter();
        buf.put_u32_le(elems.len() as u32);

        elems.for_each(|field_value| {
            new_serialize_datum(inner_type, field_value, buf);
        });
    }

    // Different logic from `plain`:
    //
    // We don't reuse the serialization of `struct<k, v>[]` for map, as it introduces overhead
    // for column ids of `struct<k, v>`. It's unnecessary because the shape of the map is fixed.
    fn new_serialize_map(map_type: &MapType, value: MapRef<'_>, buf: &mut impl BufMut) {
        let elems = value.iter();
        buf.put_u32_le(elems.len() as u32);

        elems.for_each(|(k, v)| {
            new_serialize_scalar(map_type.key(), k, buf);
            new_serialize_datum(map_type.value(), v, buf);
        });
    }

    // Copy of `plain` but call `new_` for composite types.
    pub fn new_serialize_scalar(
        data_type: &DataType,
        value: ScalarRefImpl<'_>,
        buf: &mut impl BufMut,
    ) {
        match value {
            ScalarRefImpl::Struct(s) => new_serialize_struct(data_type.as_struct(), s, buf),
            ScalarRefImpl::List(l) => new_serialize_list(data_type.as_list_element_type(), l, buf),
            ScalarRefImpl::Map(m) => new_serialize_map(data_type.as_map(), m, buf),

            _ => plain::serialize_scalar(value, buf),
        }
    }

    // --- deserialize ---

    // Copy of `plain` but call `new_` for the scalar.
    fn new_inner_deserialize_datum(data: &mut &[u8], ty: &DataType) -> Result<Datum> {
        let null_tag = data.get_u8();
        match null_tag {
            0 => Ok(None),
            1 => Some(new_deserialize_scalar(ty, data)).transpose(),
            _ => Err(ValueEncodingError::InvalidTagEncoding(null_tag)),
        }
    }

    // Different logic from `plain`:
    //
    // Recursively construct a new column-aware `Deserializer` for nested fields.
    fn new_deserialize_struct(struct_def: &StructType, data: &mut &[u8]) -> Result<ScalarImpl> {
        let deserializer = super::Deserializer::from_struct(struct_def.clone()); // cloning `StructType` is lightweight
        let encoded_len = data.get_u32_le() as usize;

        let (struct_data, remaining) = data.split_at(encoded_len);
        *data = remaining;
        let fields = deserializer.deserialize(struct_data)?;

        Ok(ScalarImpl::Struct(StructValue::new(fields)))
    }

    // Copy of `plain` but call `new_` for the elements.
    fn new_deserialize_list(item_type: &DataType, data: &mut &[u8]) -> Result<ScalarImpl> {
        let len = data.get_u32_le();
        let mut builder = item_type.create_array_builder(len as usize);
        for _ in 0..len {
            builder.append(new_inner_deserialize_datum(data, item_type)?);
        }
        Ok(ScalarImpl::List(ListValue::new(builder.finish())))
    }

    // Different logic from `plain`:
    //
    // We don't reuse the deserialization of `struct<k, v>[]` for map, as it introduces overhead
    // for column ids of `struct<k, v>`. It's unnecessary because the shape of the map is fixed.
    fn new_deserialize_map(map_type: &MapType, data: &mut &[u8]) -> Result<ScalarImpl> {
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

    // Copy of `plain` but call `new_` for composite types.
    pub fn new_deserialize_scalar(ty: &DataType, data: &mut &[u8]) -> Result<ScalarImpl> {
        Ok(match ty {
            DataType::Struct(struct_def) => new_deserialize_struct(struct_def, data)?,
            DataType::List(item_type) => new_deserialize_list(item_type, data)?,
            DataType::Map(map_type) => new_deserialize_map(map_type, data)?,
            DataType::Vector(_) => todo!("VECTOR_PLACEHOLDER"),

            data_types::simple!() => plain::deserialize_value(ty, data)?,
        })
    }
}

/// When a row has columns no more than this number, we will use stack for some intermediate buffers.
const COLUMN_ON_STACK: usize = 8;

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
    offsets: SmallVec<[u8; COLUMN_ON_STACK * 2]>,
    data: Vec<u8>,
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
            // Use different encoding logic for alterable types.
            if data_type.can_alter() == Some(true) {
                new_serde::new_serialize_scalar(data_type, v, data);
            } else {
                plain::serialize_scalar(v, data);
            }
        }
    }
}

impl Encode for Option<&[u8]> {
    fn encode_to(self, _data_type: &DataType, data: &mut Vec<u8>) {
        // Already encoded, just copy the bytes.
        if let Some(v) = self {
            data.extend(v);
        }
    }
}

impl RowEncoding {
    fn new() -> Self {
        RowEncoding {
            header: Header::new(),
            offsets: Default::default(),
            data: Default::default(),
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
            .resize(usize_offsets.len() * offset_width.width(), 0);

        let mut offsets_buf = &mut self.offsets[..];
        for &offset in usize_offsets {
            offsets_buf.put_uint_le(offset as u64, offset_width.width());
        }
    }

    fn encode<T: Encode>(
        &mut self,
        datums: impl IntoIterator<Item = T>,
        data_types: impl IntoIterator<Item = &DataType>,
    ) {
        debug_assert!(
            self.data.is_empty(),
            "should not encode one RowEncoding object multiple times."
        );
        let datums = datums.into_iter();
        let mut offset_usize =
            SmallVec::<[usize; COLUMN_ON_STACK]>::with_capacity(datums.size_hint().0);
        for (datum, data_type) in datums.zip_eq_debug(data_types) {
            offset_usize.push(self.data.len());
            datum.encode_to(data_type, &mut self.data);
        }
        self.set_offsets(&offset_usize);
    }
}

mod data_types {
    use crate::types::{DataType, StructType};

    /// A trait unifying data types of a row and field types of a struct.
    pub trait DataTypes: Clone {
        fn iter(&self) -> impl ExactSizeIterator<Item = &DataType>;
        fn at(&self, index: usize) -> &DataType;
    }

    impl<T> DataTypes for T
    where
        T: AsRef<[DataType]> + Clone,
    {
        fn iter(&self) -> impl ExactSizeIterator<Item = &DataType> {
            self.as_ref().iter()
        }

        fn at(&self, index: usize) -> &DataType {
            &self.as_ref()[index]
        }
    }

    impl DataTypes for StructType {
        fn iter(&self) -> impl ExactSizeIterator<Item = &DataType> {
            self.types()
        }

        fn at(&self, index: usize) -> &DataType {
            self.type_at(index)
        }
    }
}
use data_types::DataTypes;

/// Column-Aware `Serializer` holds schema related information, and shall be
/// created again once the schema changes
#[derive(Clone)]
pub struct Serializer<D: DataTypes = Vec<DataType>> {
    encoded_column_ids: EncodedColumnIds,
    data_types: D,
}

type EncodedColumnIds = SmallVec<[u8; COLUMN_ON_STACK * 4]>;

fn encode_column_ids(column_ids: impl ExactSizeIterator<Item = ColumnId>) -> EncodedColumnIds {
    // currently we hard-code ColumnId as i32
    let mut encoded_column_ids = smallvec![0; column_ids.len() * 4];
    let mut buf = &mut encoded_column_ids[..];
    for id in column_ids {
        buf.put_i32_le(id.get_id());
    }
    encoded_column_ids
}

impl Serializer {
    /// Create a new `Serializer` with given `column_ids` and `data_types`.
    pub fn new(column_ids: &[ColumnId], data_types: impl IntoIterator<Item = DataType>) -> Self {
        Self {
            encoded_column_ids: encode_column_ids(column_ids.iter().copied()),
            data_types: data_types.into_iter().collect(),
        }
    }
}

impl Serializer<StructType> {
    /// Create a new `Serializer` for the fields of the given struct.
    ///
    /// Panic if the struct type does not have field ids.
    pub fn from_struct(struct_type: StructType) -> Self {
        Self {
            encoded_column_ids: encode_column_ids(struct_type.ids().unwrap()),
            data_types: struct_type,
        }
    }
}

impl<D: DataTypes> Serializer<D> {
    fn datum_num(&self) -> usize {
        self.encoded_column_ids.len() / 4
    }

    fn serialize_raw<T: Encode>(&self, datums: impl IntoIterator<Item = T>) -> Vec<u8> {
        let mut encoding = RowEncoding::new();
        encoding.encode(datums, self.data_types.iter());
        self.finish(encoding)
    }

    fn finish(&self, encoding: RowEncoding) -> Vec<u8> {
        let mut row_bytes = Vec::with_capacity(
            5 + self.encoded_column_ids.len() + encoding.offsets.len() + encoding.data.len(), /* 5 comes from u8+u32 */
        );
        row_bytes.put_u8(encoding.header.into_bits());
        row_bytes.put_u32_le(self.datum_num() as u32);
        row_bytes.extend(&self.encoded_column_ids);
        row_bytes.extend(&encoding.offsets);
        row_bytes.extend(&encoding.data);

        row_bytes
    }
}

impl<D: DataTypes> ValueRowSerializer for Serializer<D> {
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
pub struct Deserializer<D: DataTypes = Arc<[DataType]>> {
    mapping: ColumnMapping,
    data_types: D,

    /// A row with default values for each column.
    ///
    /// `None` if all default values are `NULL`, typically for struct fields.
    default_row: Option<Vec<Datum>>,
}

/// A mapping from column id to the index of the column in the schema.
#[derive(Clone)]
enum ColumnMapping {
    /// For small number of columns, use linear search with `SmallVec`. This ensures no heap allocation.
    Small(SmallVec<[i32; COLUMN_ON_STACK]>),
    /// For larger number of columns, build a `HashMap` for faster lookup.
    Large(HashMap<i32, usize>),
}

impl ColumnMapping {
    fn new(column_ids: impl ExactSizeIterator<Item = ColumnId>) -> Self {
        if column_ids.len() <= COLUMN_ON_STACK {
            Self::Small(column_ids.map(|c| c.get_id()).collect())
        } else {
            Self::Large(
                column_ids
                    .enumerate()
                    .map(|(i, c)| (c.get_id(), i))
                    .collect(),
            )
        }
    }

    fn get(&self, id: i32) -> Option<usize> {
        match self {
            ColumnMapping::Small(vec) => vec.iter().position(|&x| x == id),
            ColumnMapping::Large(map) => map.get(&id).copied(),
        }
    }
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
            mapping: ColumnMapping::new(column_ids.iter().copied()),
            data_types: schema,
            default_row: Some(default_row),
        }
    }
}

impl Deserializer<StructType> {
    /// Create a new `Deserializer` for the fields of the given struct.
    ///
    /// Panic if the struct type does not have field ids.
    pub fn from_struct(struct_type: StructType) -> Self {
        Self {
            mapping: ColumnMapping::new(struct_type.ids().unwrap()),
            data_types: struct_type,
            default_row: None,
        }
    }
}

impl<D: DataTypes> ValueRowDeserializer for Deserializer<D> {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        let encoded_bytes = EncodedBytes::new(encoded_bytes)?;

        let mut row =
            (self.default_row.clone()).unwrap_or_else(|| vec![None; self.data_types.iter().len()]);

        for (id, mut data) in encoded_bytes {
            let Some(decoded_idx) = self.mapping.get(id) else {
                continue;
            };
            let data_type = self.data_types.at(decoded_idx);

            let datum = if data.is_empty() {
                None
            } else if data_type.can_alter() == Some(true) {
                Some(new_serde::new_deserialize_scalar(data_type, &mut data)?)
            } else {
                Some(plain::deserialize_value(data_type, &mut data)?)
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
// TODO: we only support trimming dropped top-level columns here; also support nested fields.
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

    // Data types are only needed when we are actually serializing. But we have encoded data here.
    // Simple pass dummy data types.
    let dummy_data_types = vec![DataType::Boolean; column_ids.len()];

    let row_bytes = Serializer::new(&column_ids, dummy_data_types).serialize_raw(datums);
    Some(row_bytes)
}
