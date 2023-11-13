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

use std::cmp::Ordering;
use std::fmt::{self, Debug, Display};
use std::future::Future;
use std::mem::size_of;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_pb::data::{ListArrayData, PbArray, PbArrayType};
use serde::{Deserialize, Serializer};

use super::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayResult, BoolArray, PrimitiveArray,
    PrimitiveArrayItemType, RowRef, Utf8Array,
};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;
use crate::row::Row;
use crate::types::{hash_datum, DataType, DatumRef, DefaultOrd, Scalar, ScalarRefImpl, ToText};
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
    /// Returns the total number of elements in the flattened array.
    pub fn flatten_len(&self) -> usize {
        self.value.len()
    }

    /// Flatten the list array into a single array.
    ///
    /// # Example
    /// ```text
    /// [[1,2,3],NULL,[4,5]] => [1,2,3,4,5]
    /// ```
    pub fn flatten(&self) -> ArrayImpl {
        (*self.value).clone()
    }

    pub fn from_protobuf(array: &PbArray) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.values.is_empty(),
            "Must have no buffer in a list array"
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

    pub fn len(&self) -> usize {
        self.values.len()
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
        datatype: &DataType,
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        let mut inner_deserializer = memcomparable::Deserializer::new(bytes.as_slice());
        let mut builder = datatype.create_array_builder(0);
        while inner_deserializer.has_remaining() {
            builder.append(memcmp_encoding::deserialize_datum_in_composite(
                datatype,
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
    pub fn flatten(self) -> Vec<DatumRef<'a>> {
        todo!()
    }

    /// Returns the total number of elements in the flattened list.
    pub fn flatten_len(self) -> usize {
        todo!()
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
                        || s.to_ascii_lowercase() == "null"
                        || s.contains([
                            '"', '\\', '{', '}', ',',
                            // PostgreSQL `array_isspace` includes '\x0B' but rust
                            // [`char::is_ascii_whitespace`] does not.
                            ' ', '\t', '\n', '\r', '\x0B', '\x0C',
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
}
