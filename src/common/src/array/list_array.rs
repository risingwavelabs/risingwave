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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::{self, Debug, Display};
use std::future::Future;
use std::mem::size_of;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::{ListArrayData, PbArray, PbArrayType};
use serde::{Deserialize, Serializer};
use thiserror_ext::AsReport;

use super::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayResult, BoolArray, PrimitiveArray,
    PrimitiveArrayItemType, RowRef, Utf8Array,
};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::row::Row;
use crate::types::{
    DataType, Datum, DatumRef, DefaultOrd, Scalar, ScalarImpl, ScalarRefImpl, ToDatumRef, ToText,
    hash_datum,
};
use crate::util::memcmp_encoding;
use crate::util::value_encoding::estimate_serialize_datum_size;

#[derive(Debug, Clone, EstimateSize)]
pub struct ListArrayBuilder {
    bitmap: BitmapBuilder,
    offsets: Vec<u32>,
    value: Box<ArrayBuilderImpl>,
    len: usize,
}

impl ArrayBuilder for ListArrayBuilder {
    type ArrayType = ListArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Self {
        panic!("please use `ListArrayBuilder::with_type` instead");
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        // TODO: deprecate this
        Self::with_type(
            capacity,
            // Default datatype
            DataType::List(Box::new(DataType::Int16)),
        )
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        let DataType::List(value_type) = ty else {
            panic!("data type must be DataType::List");
        };
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            offsets,
            value: Box::new(value_type.create_array_builder(capacity)),
            len: 0,
        }
    }

    fn append_n(&mut self, n: usize, value: Option<ListRef<'_>>) {
        match value {
            None => {
                self.bitmap.append_n(n, false);
                let last = *self.offsets.last().unwrap();
                for _ in 0..n {
                    self.offsets.push(last);
                }
            }
            Some(v) => {
                self.bitmap.append_n(n, true);
                for _ in 0..n {
                    let last = *self.offsets.last().unwrap();
                    let elems = v.iter();
                    self.offsets.push(
                        last.checked_add(elems.len() as u32)
                            .expect("offset overflow"),
                    );
                    for elem in elems {
                        self.value.append(elem);
                    }
                }
            }
        }
        self.len += n;
    }

    fn append_array(&mut self, other: &ListArray) {
        self.bitmap.append_bitmap(&other.bitmap);
        let last = *self.offsets.last().unwrap();
        self.offsets
            .append(&mut other.offsets[1..].iter().map(|o| *o + last).collect());
        self.value.append_array(&other.value);
        self.len += other.len();
    }

    fn pop(&mut self) -> Option<()> {
        self.bitmap.pop()?;
        let start = self.offsets.pop().unwrap();
        let end = *self.offsets.last().unwrap();
        self.len -= 1;
        for _ in end..start {
            self.value.pop().unwrap();
        }
        Some(())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> ListArray {
        ListArray {
            bitmap: self.bitmap.finish(),
            offsets: self.offsets.into(),
            value: Box::new(self.value.finish()),
        }
    }
}

impl ListArrayBuilder {
    pub fn append_row_ref(&mut self, row: RowRef<'_>) {
        self.bitmap.append(true);
        let last = *self.offsets.last().unwrap();
        self.offsets
            .push(last.checked_add(row.len() as u32).expect("offset overflow"));
        self.len += 1;
        for v in row.iter() {
            self.value.append(v);
        }
    }
}

/// Each item of this `ListArray` is a `List<T>`, or called `T[]` (T array).
///
/// * As other arrays, there is a null bitmap, with `1` meaning nonnull and `0` meaning null.
/// * As [`super::BytesArray`], there is an offsets `Vec` and a value `Array`. The value `Array` has
///   all items concatenated, and the offsets `Vec` stores start and end indices into it for
///   slicing. Effectively, the inner array is the flattened form, and `offsets.len() == n + 1`.
///
/// For example, `values (array[1]), (array[]::int[]), (null), (array[2, 3]);` stores an inner
///  `I32Array` with `[1, 2, 3]`, along with offsets `[0, 1, 1, 1, 3]` and null bitmap `TTFT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListArray {
    pub(super) bitmap: Bitmap,
    pub(super) offsets: Box<[u32]>,
    pub(super) value: Box<ArrayImpl>,
}

impl EstimateSize for ListArray {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size()
            + self.offsets.len() * size_of::<u32>()
            + self.value.estimated_size()
    }
}

impl Array for ListArray {
    type Builder = ListArrayBuilder;
    type OwnedItem = ListValue;
    type RefItem<'a> = ListRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        ListRef {
            array: &self.value,
            start: *self.offsets.get_unchecked(idx),
            end: *self.offsets.get_unchecked(idx + 1),
        }
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let value = self.value.to_protobuf();
        PbArray {
            array_type: PbArrayType::List as i32,
            struct_array_data: None,
            list_array_data: Some(Box::new(ListArrayData {
                offsets: self.offsets.to_vec(),
                value: Some(Box::new(value)),
                value_type: Some(self.value.data_type().to_protobuf()),
            })),
            null_bitmap: Some(self.bitmap.to_protobuf()),
            values: vec![],
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    fn data_type(&self) -> DataType {
        DataType::List(Box::new(self.value.data_type()))
    }
}

impl ListArray {
    /// Flatten the list array into a single array.
    ///
    /// # Example
    ///
    /// ```text
    /// [[1,2,3],NULL,[4,5]] => [1,2,3,4,5]
    /// [[[1],[2]],[[3],[4]]] => [1,2,3,4]
    /// ```
    pub fn flatten(&self) -> ArrayImpl {
        match &*self.value {
            ArrayImpl::List(inner) => inner.flatten(),
            a => a.clone(),
        }
    }

    /// Return the inner array of the list array.
    pub fn values(&self) -> &ArrayImpl {
        &self.value
    }

    pub fn from_protobuf(array: &PbArray) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.values.is_empty(),
            "Must have no buffer in a list array"
        );
        debug_assert!(
            (array.array_type == PbArrayType::List as i32)
                || (array.array_type == PbArrayType::Map as i32),
            "invalid array type for list: {}",
            array.array_type
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        let array_data = array.get_list_array_data()?.to_owned();
        let flatten_len = match array_data.offsets.last() {
            Some(&n) => n as usize,
            None => bail!("Must have at least one element in offsets"),
        };
        let value = ArrayImpl::from_protobuf(array_data.value.as_ref().unwrap(), flatten_len)?;
        let arr = ListArray {
            bitmap,
            offsets: array_data.offsets.into(),
            value: Box::new(value),
        };
        Ok(arr.into())
    }

    /// Apply the function on the underlying elements.
    /// e.g. `map_inner([[1,2,3],NULL,[4,5]], DOUBLE) = [[2,4,6],NULL,[8,10]]`
    pub async fn map_inner<E, Fut, F>(self, f: F) -> std::result::Result<ListArray, E>
    where
        F: FnOnce(ArrayImpl) -> Fut,
        Fut: Future<Output = std::result::Result<ArrayImpl, E>>,
    {
        let new_value = (f)(*self.value).await?;

        Ok(Self {
            offsets: self.offsets,
            bitmap: self.bitmap,
            value: Box::new(new_value),
        })
    }

    /// Returns the offsets of this list.
    ///
    /// # Example
    /// ```text
    /// list    = [[a, b, c], [], NULL, [d], [NULL, f]]
    /// offsets = [0, 3, 3, 3, 4, 6]
    /// ```
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }
}

impl<T, L> FromIterator<Option<L>> for ListArray
where
    T: PrimitiveArrayItemType,
    L: IntoIterator<Item = T>,
{
    fn from_iter<I: IntoIterator<Item = Option<L>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = ListArrayBuilder::with_type(
            iter.size_hint().0,
            DataType::List(Box::new(T::DATA_TYPE.clone())),
        );
        for v in iter {
            match v {
                None => builder.append(None),
                Some(v) => {
                    builder.append(Some(v.into_iter().collect::<ListValue>().as_scalar_ref()))
                }
            }
        }
        builder.finish()
    }
}

impl FromIterator<ListValue> for ListArray {
    fn from_iter<I: IntoIterator<Item = ListValue>>(iter: I) -> Self {
        let mut iter = iter.into_iter();
        let first = iter.next().expect("empty iterator");
        let mut builder = ListArrayBuilder::with_type(
            iter.size_hint().0,
            DataType::List(Box::new(first.data_type())),
        );
        builder.append(Some(first.as_scalar_ref()));
        for v in iter {
            builder.append(Some(v.as_scalar_ref()));
        }
        builder.finish()
    }
}

#[derive(Clone, PartialEq, Eq, EstimateSize)]
pub struct ListValue {
    values: Box<ArrayImpl>,
}

impl Debug for ListValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_scalar_ref().fmt(f)
    }
}

impl Display for ListValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_scalar_ref().write(f)
    }
}

impl ListValue {
    pub fn new(values: ArrayImpl) -> Self {
        Self {
            values: Box::new(values),
        }
    }

    pub fn into_array(self) -> ArrayImpl {
        *self.values
    }

    pub fn empty(datatype: &DataType) -> Self {
        Self::new(datatype.create_array_builder(0).finish())
    }

    /// Creates a new `ListValue` from an iterator of `Datum`.
    pub fn from_datum_iter<T: ToDatumRef>(
        elem_datatype: &DataType,
        iter: impl IntoIterator<Item = T>,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = elem_datatype.create_array_builder(iter.size_hint().0);
        for datum in iter {
            builder.append(datum);
        }
        Self::new(builder.finish())
    }

    /// Returns the length of the list.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if the list has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterates over the elements of the list.
    pub fn iter(&self) -> impl DoubleEndedIterator + ExactSizeIterator<Item = DatumRef<'_>> {
        self.values.iter()
    }

    /// Get the element at the given index. Returns `None` if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<DatumRef<'_>> {
        if index < self.len() {
            Some(self.values.value_at(index))
        } else {
            None
        }
    }

    /// Returns the data type of the elements in the list.
    pub fn data_type(&self) -> DataType {
        self.values.data_type()
    }

    pub fn memcmp_deserialize(
        item_datatype: &DataType,
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        let mut inner_deserializer = memcomparable::Deserializer::new(bytes.as_slice());
        let mut builder = item_datatype.create_array_builder(0);
        while inner_deserializer.has_remaining() {
            builder.append(memcmp_encoding::deserialize_datum_in_composite(
                item_datatype,
                &mut inner_deserializer,
            )?)
        }
        Ok(Self::new(builder.finish()))
    }

    // Used to display ListValue in explain for better readibilty.
    pub fn display_for_explain(&self) -> String {
        // Example of ListValue display: ARRAY[1, 2, null]
        format!(
            "ARRAY[{}]",
            self.iter()
                .map(|v| {
                    match v.as_ref() {
                        None => "null".into(),
                        Some(scalar) => scalar.to_text(),
                    }
                })
                .format(", ")
        )
    }

    /// Returns a mutable slice if the list is of type `int64[]`.
    pub fn as_i64_mut_slice(&mut self) -> Option<&mut [i64]> {
        match self.values.as_mut() {
            ArrayImpl::Int64(array) => Some(array.as_mut_slice()),
            _ => None,
        }
    }
}

impl PartialOrd for ListValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ListValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_scalar_ref().cmp(&other.as_scalar_ref())
    }
}

impl<T: PrimitiveArrayItemType> FromIterator<Option<T>> for ListValue {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<PrimitiveArray<T>>().into())
    }
}

impl<T: PrimitiveArrayItemType> FromIterator<T> for ListValue {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<PrimitiveArray<T>>().into())
    }
}

impl FromIterator<bool> for ListValue {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<BoolArray>().into())
    }
}

impl<'a> FromIterator<Option<&'a str>> for ListValue {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<Utf8Array>().into())
    }
}

impl<'a> FromIterator<&'a str> for ListValue {
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<Utf8Array>().into())
    }
}

impl FromIterator<ListValue> for ListValue {
    fn from_iter<I: IntoIterator<Item = ListValue>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect::<ListArray>().into())
    }
}

impl From<ListValue> for ArrayImpl {
    fn from(value: ListValue) -> Self {
        *value.values
    }
}

/// A slice of an array
#[derive(Copy, Clone)]
pub struct ListRef<'a> {
    array: &'a ArrayImpl,
    start: u32,
    end: u32,
}

impl<'a> ListRef<'a> {
    /// Returns the length of the list.
    pub fn len(&self) -> usize {
        (self.end - self.start) as usize
    }

    /// Returns `true` if the list has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    /// Returns the data type of the elements in the list.
    pub fn data_type(&self) -> DataType {
        self.array.data_type()
    }

    /// Returns the elements in the flattened list.
    pub fn flatten(self) -> ListRef<'a> {
        match self.array {
            ArrayImpl::List(inner) => ListRef {
                array: &inner.value,
                start: inner.offsets[self.start as usize],
                end: inner.offsets[self.end as usize],
            }
            .flatten(),
            _ => self,
        }
    }

    /// Iterates over the elements of the list.
    pub fn iter(self) -> impl DoubleEndedIterator + ExactSizeIterator<Item = DatumRef<'a>> + 'a {
        (self.start..self.end).map(|i| self.array.value_at(i as usize))
    }

    /// Get the element at the given index. Returns `None` if the index is out of bounds.
    pub fn get(self, index: usize) -> Option<DatumRef<'a>> {
        if index < self.len() {
            Some(self.array.value_at(self.start as usize + index))
        } else {
            None
        }
    }

    pub fn memcmp_serialize(
        self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        let mut inner_serializer = memcomparable::Serializer::new(vec![]);
        for datum_ref in self.iter() {
            memcmp_encoding::serialize_datum_in_composite(datum_ref, &mut inner_serializer)?
        }
        serializer.serialize_bytes(&inner_serializer.into_inner())
    }

    pub fn hash_scalar_inner<H: std::hash::Hasher>(self, state: &mut H) {
        for datum_ref in self.iter() {
            hash_datum(datum_ref, state);
        }
    }

    /// estimate the serialized size with value encoding
    pub fn estimate_serialize_size_inner(self) -> usize {
        self.iter().map(estimate_serialize_datum_size).sum()
    }

    pub fn to_owned(self) -> ListValue {
        let mut builder = self.array.create_builder(self.len());
        for datum_ref in self.iter() {
            builder.append(datum_ref);
        }
        ListValue::new(builder.finish())
    }

    /// Returns a slice if the list is of type `int64[]`.
    pub fn as_i64_slice(&self) -> Option<&[i64]> {
        match &self.array {
            ArrayImpl::Int64(array) => {
                Some(&array.as_slice()[self.start as usize..self.end as usize])
            }
            _ => None,
        }
    }

    /// # Panics
    /// Panics if the list is not a map's internal representation (See [`super::MapArray`]).
    pub(super) fn as_map_kv(self) -> (ListRef<'a>, ListRef<'a>) {
        let (k, v) = self.array.as_struct().fields().collect_tuple().unwrap();
        (
            ListRef {
                array: k,
                start: self.start,
                end: self.end,
            },
            ListRef {
                array: v,
                start: self.start,
                end: self.end,
            },
        )
    }
}

impl PartialEq for ListRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for ListRef<'_> {}

impl PartialOrd for ListRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ListRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.iter().cmp_by(other.iter(), |a, b| a.default_cmp(&b))
    }
}

impl Debug for ListRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl Row for ListRef<'_> {
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        self.array.value_at(self.start as usize + index)
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        self.array.value_at_unchecked(self.start as usize + index)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
        (*self).iter()
    }
}

impl ToText for ListRef<'_> {
    // This function will be invoked when pgwire prints a list value in string.
    // Refer to PostgreSQL `array_out` or `appendPGArray`.
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.iter().format_with(",", |datum_ref, f| {
                let s = datum_ref.to_text();
                // Never quote null or inner list, but quote empty, verbatim 'null', special
                // chars and whitespaces.
                let need_quote = !matches!(datum_ref, None | Some(ScalarRefImpl::List(_)))
                    && (s.is_empty()
                        || s.eq_ignore_ascii_case("null")
                        || s.contains([
                            '"', '\\', ',',
                            // whilespace:
                            // PostgreSQL `array_isspace` includes '\x0B' but rust
                            // [`char::is_ascii_whitespace`] does not.
                            ' ', '\t', '\n', '\r', '\x0B', '\x0C', // list-specific:
                            '{', '}',
                        ]));
                if need_quote {
                    f(&"\"")?;
                    s.chars().try_for_each(|c| {
                        if c == '"' || c == '\\' {
                            f(&"\\")?;
                        }
                        f(&c)
                    })?;
                    f(&"\"")
                } else {
                    f(&s)
                }
            })
        )
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            DataType::List { .. } => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl<'a> From<&'a ListValue> for ListRef<'a> {
    fn from(value: &'a ListValue) -> Self {
        ListRef {
            array: &value.values,
            start: 0,
            end: value.len() as u32,
        }
    }
}

impl From<ListRef<'_>> for ListValue {
    fn from(value: ListRef<'_>) -> Self {
        value.to_owned()
    }
}

impl ListValue {
    /// Construct an array from literal string.
    pub fn from_str(input: &str, data_type: &DataType) -> Result<Self, String> {
        struct Parser<'a> {
            input: &'a str,
            data_type: &'a DataType,
        }

        impl Parser<'_> {
            /// Parse a datum.
            fn parse(&mut self) -> Result<Datum, String> {
                self.skip_whitespace();
                if self.data_type.is_array() {
                    if self.try_parse_null() {
                        return Ok(None);
                    }
                    Ok(Some(self.parse_array()?.into()))
                } else {
                    self.parse_value()
                }
            }

            /// Parse an array.
            fn parse_array(&mut self) -> Result<ListValue, String> {
                self.skip_whitespace();
                if !self.try_consume('{') {
                    return Err("Array value must start with \"{\"".to_owned());
                }
                self.skip_whitespace();
                if self.try_consume('}') {
                    return Ok(ListValue::empty(self.data_type.as_list()));
                }
                let mut builder = ArrayBuilderImpl::with_type(0, self.data_type.as_list().clone());
                loop {
                    let mut parser = Self {
                        input: self.input,
                        data_type: self.data_type.as_list(),
                    };
                    builder.append(parser.parse()?);
                    self.input = parser.input;

                    // expect ',' or '}'
                    self.skip_whitespace();
                    match self.peek() {
                        Some(',') => {
                            self.try_consume(',');
                        }
                        Some('}') => {
                            self.try_consume('}');
                            break;
                        }
                        None => return Err(Self::eoi()),
                        _ => return Err("Unexpected array element.".to_owned()),
                    }
                }
                Ok(ListValue::new(builder.finish()))
            }

            /// Parse a non-array value.
            fn parse_value(&mut self) -> Result<Datum, String> {
                if self.peek() == Some('"') {
                    return Ok(Some(self.parse_quoted()?));
                }
                // peek until the next unescaped ',' or '}'
                let mut chars = self.input.char_indices();
                let mut has_escape = false;
                let s = loop {
                    match chars.next().ok_or_else(Self::eoi)? {
                        (_, '\\') => {
                            has_escape = true;
                            chars.next().ok_or_else(Self::eoi)?;
                        }
                        (i, c @ ',' | c @ '}') => {
                            let s = &self.input[..i];
                            // consume the value and leave the ',' or '}' for parent
                            self.input = &self.input[i..];

                            break if has_escape {
                                Cow::Owned(Self::unescape_trim_end(s))
                            } else {
                                let trimmed = s.trim_end();
                                if trimmed.is_empty() {
                                    return Err(format!("Unexpected \"{c}\" character."));
                                }
                                if trimmed.eq_ignore_ascii_case("null") {
                                    return Ok(None);
                                }
                                Cow::Borrowed(trimmed)
                            };
                        }
                        (_, '{') => return Err("Unexpected \"{\" character.".to_owned()),
                        (_, '"') => return Err("Unexpected array element.".to_owned()),
                        _ => {}
                    }
                };
                Ok(Some(
                    ScalarImpl::from_text(&s, self.data_type).map_err(|e| e.to_report_string())?,
                ))
            }

            /// Parse a double quoted non-array value.
            fn parse_quoted(&mut self) -> Result<ScalarImpl, String> {
                assert!(self.try_consume('"'));
                // peek until the next unescaped '"'
                let mut chars = self.input.char_indices();
                let mut has_escape = false;
                let s = loop {
                    match chars.next().ok_or_else(Self::eoi)? {
                        (_, '\\') => {
                            has_escape = true;
                            chars.next().ok_or_else(Self::eoi)?;
                        }
                        (i, '"') => {
                            let s = &self.input[..i];
                            self.input = &self.input[i + 1..];
                            break if has_escape {
                                Cow::Owned(Self::unescape(s))
                            } else {
                                Cow::Borrowed(s)
                            };
                        }
                        _ => {}
                    }
                };
                ScalarImpl::from_text(&s, self.data_type).map_err(|e| e.to_report_string())
            }

            /// Unescape a string.
            fn unescape(s: &str) -> String {
                let mut unescaped = String::with_capacity(s.len());
                let mut chars = s.chars();
                while let Some(mut c) = chars.next() {
                    if c == '\\' {
                        c = chars.next().unwrap();
                    }
                    unescaped.push(c);
                }
                unescaped
            }

            /// Unescape a string and trim the trailing whitespaces.
            ///
            /// Example: `"\  " -> " "`
            fn unescape_trim_end(s: &str) -> String {
                let mut unescaped = String::with_capacity(s.len());
                let mut chars = s.chars();
                let mut len_after_last_escaped_char = 0;
                while let Some(mut c) = chars.next() {
                    if c == '\\' {
                        c = chars.next().unwrap();
                        unescaped.push(c);
                        len_after_last_escaped_char = unescaped.len();
                    } else {
                        unescaped.push(c);
                    }
                }
                let l = unescaped[len_after_last_escaped_char..].trim_end().len();
                unescaped.truncate(len_after_last_escaped_char + l);
                unescaped
            }

            /// Consume the next 4 characters if it matches "null".
            ///
            /// Note: We don't use this function when parsing non-array values.
            ///       Because we can't decide whether it is a null value or a string starts with "null".
            ///       Consider this case: `{null value}` => `["null value"]`
            fn try_parse_null(&mut self) -> bool {
                if let Some(s) = self.input.get(..4)
                    && s.eq_ignore_ascii_case("null")
                {
                    let next_char = self.input[4..].chars().next();
                    match next_char {
                        None | Some(',' | '}') => {}
                        Some(c) if c.is_ascii_whitespace() => {}
                        // following normal characters
                        _ => return false,
                    }
                    self.input = &self.input[4..];
                    true
                } else {
                    false
                }
            }

            /// Consume the next character if it matches `c`.
            fn try_consume(&mut self, c: char) -> bool {
                if self.peek() == Some(c) {
                    self.input = &self.input[c.len_utf8()..];
                    true
                } else {
                    false
                }
            }

            /// Expect end of input.
            fn expect_end(&mut self) -> Result<(), String> {
                self.skip_whitespace();
                match self.peek() {
                    Some(_) => Err("Junk after closing right brace.".to_owned()),
                    None => Ok(()),
                }
            }

            /// Skip whitespaces.
            fn skip_whitespace(&mut self) {
                self.input = match self
                    .input
                    .char_indices()
                    .find(|(_, c)| !c.is_ascii_whitespace())
                {
                    Some((i, _)) => &self.input[i..],
                    None => "",
                };
            }

            /// Peek the next character.
            fn peek(&self) -> Option<char> {
                self.input.chars().next()
            }

            /// Return the error message for unexpected end of input.
            fn eoi() -> String {
                "Unexpected end of input.".into()
            }
        }

        let mut parser = Parser { input, data_type };
        let array = parser.parse_array()?;
        parser.expect_end()?;
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use more_asserts::{assert_gt, assert_lt};

    use super::*;

    #[test]
    fn test_protobuf() {
        use crate::array::*;
        let array = ListArray::from_iter([
            Some(vec![12i32, -7, 25]),
            None,
            Some(vec![0, -127, 127, 50]),
            Some(vec![]),
        ]);
        let actual = ListArray::from_protobuf(&array.to_protobuf()).unwrap();
        assert_eq!(actual, ArrayImpl::List(array));
    }

    #[test]
    fn test_append_array() {
        let part1 = ListArray::from_iter([Some([12i32, -7, 25]), None]);
        let part2 = ListArray::from_iter([Some(vec![0, -127, 127, 50]), Some(vec![])]);

        let mut builder = ListArrayBuilder::with_type(4, DataType::List(Box::new(DataType::Int32)));
        builder.append_array(&part1);
        builder.append_array(&part2);

        let expected = ListArray::from_iter([
            Some(vec![12i32, -7, 25]),
            None,
            Some(vec![0, -127, 127, 50]),
            Some(vec![]),
        ]);
        assert_eq!(builder.finish(), expected);
    }

    // Ensure `create_builder` exactly copies the same metadata.
    #[test]
    fn test_list_create_builder() {
        use crate::array::*;
        let arr = ListArray::from_iter([Some([F32::from(2.0), F32::from(42.0), F32::from(1.0)])]);
        let arr2 = arr.create_builder(0).finish();
        assert_eq!(arr.data_type(), arr2.data_type());
    }

    #[test]
    fn test_builder_pop() {
        use crate::array::*;

        {
            let mut builder =
                ListArrayBuilder::with_type(1, DataType::List(Box::new(DataType::Int32)));
            let val = ListValue::from_iter([1i32, 2, 3]);
            builder.append(Some(val.as_scalar_ref()));
            assert!(builder.pop().is_some());
            assert!(builder.pop().is_none());
            let arr = builder.finish();
            assert!(arr.is_empty());
        }

        {
            let data_type = DataType::List(Box::new(DataType::List(Box::new(DataType::Int32))));
            let mut builder = ListArrayBuilder::with_type(2, data_type);
            let val1 = ListValue::from_iter([1, 2, 3]);
            let val2 = ListValue::from_iter([1, 2, 3]);
            let list1 = ListValue::from_iter([val1, val2]);
            builder.append(Some(list1.as_scalar_ref()));

            let val3 = ListValue::from_iter([1, 2, 3]);
            let val4 = ListValue::from_iter([1, 2, 3]);
            let list2 = ListValue::from_iter([val3, val4]);

            builder.append(Some(list2.as_scalar_ref()));

            assert!(builder.pop().is_some());

            let arr = builder.finish();
            assert_eq!(arr.len(), 1);
            assert_eq!(arr.value_at(0).unwrap(), list1.as_scalar_ref());
        }
    }

    #[test]
    fn test_list_nested_layout() {
        use crate::array::*;

        let listarray1 = ListArray::from_iter([Some([1i32, 2]), Some([3, 4])]);
        let listarray2 = ListArray::from_iter([Some(vec![5, 6, 7]), None, Some(vec![8])]);
        let listarray3 = ListArray::from_iter([Some([9, 10])]);

        let nestarray = ListArray::from_iter(
            [listarray1, listarray2, listarray3]
                .into_iter()
                .map(|l| ListValue::new(l.into())),
        );
        let actual = ListArray::from_protobuf(&nestarray.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::List(nestarray), actual);
    }

    #[test]
    fn test_list_value_cmp() {
        // ARRAY[1, 1] < ARRAY[1, 2, 1]
        assert_lt!(
            ListValue::from_iter([1, 1]),
            ListValue::from_iter([1, 2, 1]),
        );
        // ARRAY[1, 2] < ARRAY[1, 2, 1]
        assert_lt!(
            ListValue::from_iter([1, 2]),
            ListValue::from_iter([1, 2, 1]),
        );
        // ARRAY[1, 3] > ARRAY[1, 2, 1]
        assert_gt!(
            ListValue::from_iter([1, 3]),
            ListValue::from_iter([1, 2, 1]),
        );
        // null > 1
        assert_gt!(
            ListValue::from_iter([None::<i32>]),
            ListValue::from_iter([1]),
        );
        // ARRAY[1, 2, null] > ARRAY[1, 2, 1]
        assert_gt!(
            ListValue::from_iter([Some(1), Some(2), None]),
            ListValue::from_iter([Some(1), Some(2), Some(1)]),
        );
        // Null value in first ARRAY results into a Greater ordering regardless of the smaller ARRAY
        // length. ARRAY[1, null] > ARRAY[1, 2, 3]
        assert_gt!(
            ListValue::from_iter([Some(1), None]),
            ListValue::from_iter([Some(1), Some(2), Some(3)]),
        );
        // ARRAY[1, null] == ARRAY[1, null]
        assert_eq!(
            ListValue::from_iter([Some(1), None]),
            ListValue::from_iter([Some(1), None]),
        );
    }

    #[test]
    fn test_list_ref_display() {
        let v = ListValue::from_iter([Some(1), None]);
        assert_eq!(v.to_string(), "{1,NULL}");
    }

    #[test]
    fn test_serialize_deserialize() {
        let value = ListValue::from_iter([Some("abcd"), Some(""), None, Some("a")]);
        let list_ref = value.as_scalar_ref();
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serializer.set_reverse(true);
        list_ref.memcmp_serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        deserializer.set_reverse(true);
        assert_eq!(
            ListValue::memcmp_deserialize(&DataType::Varchar, &mut deserializer).unwrap(),
            value
        );

        let mut builder =
            ListArrayBuilder::with_type(0, DataType::List(Box::new(DataType::Varchar)));
        builder.append(Some(list_ref));
        let array = builder.finish();
        let list_ref = array.value_at(0).unwrap();
        let mut serializer = memcomparable::Serializer::new(vec![]);
        list_ref.memcmp_serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            ListValue::memcmp_deserialize(&DataType::Varchar, &mut deserializer).unwrap(),
            value
        );
    }

    #[test]
    fn test_memcomparable() {
        let cases = [
            (
                ListValue::from_iter([123, 456]),
                ListValue::from_iter([123, 789]),
            ),
            (
                ListValue::from_iter([123, 456]),
                ListValue::from_iter([123]),
            ),
            (
                ListValue::from_iter([None, Some("")]),
                ListValue::from_iter([None, None::<&str>]),
            ),
            (
                ListValue::from_iter([Some(2)]),
                ListValue::from_iter([Some(1), None, Some(3)]),
            ),
        ];

        for (lhs, rhs) in cases {
            let lhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                lhs.as_scalar_ref()
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                rhs.as_scalar_ref()
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), lhs.cmp(&rhs));
        }
    }

    #[test]
    fn test_listref() {
        use crate::array::*;
        use crate::types;

        let arr = ListArray::from_iter([Some(vec![1, 2, 3]), None, Some(vec![4, 5, 6, 7])]);

        // get 3rd ListRef from ListArray
        let list_ref = arr.value_at(2).unwrap();
        assert_eq!(list_ref, ListValue::from_iter([4, 5, 6, 7]).as_scalar_ref());

        // Get 2nd value from ListRef
        let scalar = list_ref.get(1).unwrap();
        assert_eq!(scalar, Some(types::ScalarRefImpl::Int32(5)));
    }

    #[test]
    fn test_from_to_literal() {
        #[track_caller]
        fn test(typestr: &str, input: &str, output: Option<&str>) {
            let datatype: DataType = typestr.parse().unwrap();
            let list = ListValue::from_str(input, &datatype).unwrap();
            let actual = list.as_scalar_ref().to_text();
            let output = output.unwrap_or(input);
            assert_eq!(actual, output);
        }

        #[track_caller]
        fn test_err(typestr: &str, input: &str, err: &str) {
            let datatype: DataType = typestr.parse().unwrap();
            let actual_err = ListValue::from_str(input, &datatype).unwrap_err();
            assert_eq!(actual_err, err);
        }

        test("varchar[]", "{}", None);
        test("varchar[]", "{1 2}", Some(r#"{"1 2"}"#));
        test("varchar[]", "{🥵,🤡}", None);
        test("varchar[]", r#"{aa\\bb}"#, Some(r#"{"aa\\bb"}"#));
        test("int[]", "{1,2,3}", None);
        test("varchar[]", r#"{"1,2"}"#, None);
        test("varchar[]", r#"{1, ""}"#, Some(r#"{1,""}"#));
        test("varchar[]", r#"{"\""}"#, None);
        test("varchar[]", r#"{\   }"#, Some(r#"{" "}"#));
        test("varchar[]", r#"{\\  }"#, Some(r#"{"\\"}"#));
        test("varchar[]", "{nulla}", None);
        test("varchar[]", "{null a}", Some(r#"{"null a"}"#));
        test(
            "varchar[]",
            r#"{"null", "NULL", null, NuLL}"#,
            Some(r#"{"null","NULL",NULL,NULL}"#),
        );
        test("varchar[][]", "{{1, 2, 3}, null }", Some("{{1,2,3},NULL}"));
        test(
            "varchar[][][]",
            "{{{1, 2, 3}}, {{4, 5, 6}}}",
            Some("{{{1,2,3}},{{4,5,6}}}"),
        );
        test_err("varchar[]", "()", r#"Array value must start with "{""#);
        test_err("varchar[]", "{1,", r#"Unexpected end of input."#);
        test_err("varchar[]", "{1,}", r#"Unexpected "}" character."#);
        test_err("varchar[]", "{1,,3}", r#"Unexpected "," character."#);
        test_err("varchar[]", r#"{"a""b"}"#, r#"Unexpected array element."#);
        test_err("varchar[]", r#"{}{"#, r#"Junk after closing right brace."#);
    }
}
