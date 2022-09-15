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

use core::fmt;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};

use bytes::{Buf, BufMut};
use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;
use prost::Message;
use risingwave_pb::data::{
    Array as ProstArray, ArrayType as ProstArrayType, DataType as ProstDataType, ListArrayData,
};
use risingwave_pb::expr::ListValue as ProstListValue;
use serde::{Deserializer, Serializer};

use super::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayIterator, ArrayMeta, ArrayResult,
    RowRef, NULL_VAL_FOR_HASH,
};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::types::{
    deserialize_datum_from, display_datum_ref, serialize_datum_ref_into, to_datum_ref, DataType,
    Datum, DatumRef, Scalar, ScalarImpl, ScalarRefImpl, ToOwnedDatum,
};

/// This is a naive implementation of list array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug)]
pub struct ListArrayBuilder {
    bitmap: BitmapBuilder,
    offsets: Vec<usize>,
    value: Box<ArrayBuilderImpl>,
    value_type: DataType,
    len: usize,
}

impl ArrayBuilder for ListArrayBuilder {
    type ArrayType = ListArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Self {
        panic!("Must use with_meta.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_meta(
            capacity,
            ArrayMeta::List {
                // Default datatype
                datatype: Box::new(DataType::Int16),
            },
        )
    }

    fn with_meta(capacity: usize, meta: ArrayMeta) -> Self {
        if let ArrayMeta::List { datatype } = meta {
            Self {
                bitmap: BitmapBuilder::with_capacity(capacity),
                offsets: vec![0],
                value: Box::new(datatype.create_array_builder(capacity)),
                value_type: *datatype,
                len: 0,
            }
        } else {
            panic!("must be ArrayMeta::List");
        }
    }

    fn append(&mut self, value: Option<ListRef<'_>>) {
        match value {
            None => {
                self.bitmap.append(false);
                let last = *self.offsets.last().unwrap();
                self.offsets.push(last);
            }
            Some(v) => {
                self.bitmap.append(true);
                let last = *self.offsets.last().unwrap();
                let values_ref = v.values_ref();
                self.offsets.push(last + values_ref.len());
                for f in values_ref {
                    self.value.append_datum_ref(f).unwrap();
                }
            }
        }
        self.len += 1;
    }

    fn append_array(&mut self, other: &ListArray) -> ArrayResult<()> {
        self.bitmap.append_bitmap(&other.bitmap);
        let last = *self.offsets.last().unwrap();
        self.offsets
            .append(&mut other.offsets[1..].iter().map(|o| *o + last).collect());
        self.value.append_array(&other.value)?;
        self.len += other.len();
        Ok(())
    }

    fn finish(self) -> ListArray {
        ListArray {
            bitmap: self.bitmap.finish(),
            offsets: self.offsets,
            value: Box::new(self.value.finish()),
            value_type: self.value_type,
            len: self.len,
        }
    }
}

impl ListArrayBuilder {
    pub fn append_row_ref(&mut self, row: RowRef) -> ArrayResult<()> {
        self.bitmap.append(true);
        let last = *self.offsets.last().unwrap();
        self.offsets.push(last + row.size());
        self.len += 1;
        row.values()
            .try_for_each(|v| self.value.append_datum_ref(v))
    }
}

/// This is a naive implementation of list array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug, Clone)]
pub struct ListArray {
    bitmap: Bitmap,
    offsets: Vec<usize>,
    value: Box<ArrayImpl>,
    value_type: DataType,
    len: usize,
}

impl Array for ListArray {
    type Builder = ListArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = ListValue;
    type RefItem<'a> = ListRef<'a>;

    fn value_at(&self, idx: usize) -> Option<ListRef<'_>> {
        if !self.is_null(idx) {
            Some(ListRef::Indexed { arr: self, idx })
        } else {
            None
        }
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<ListRef<'_>> {
        if !self.is_null_unchecked(idx) {
            Some(ListRef::Indexed { arr: self, idx })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> ProstArray {
        let value = self.value.to_protobuf();
        ProstArray {
            array_type: ProstArrayType::List as i32,
            struct_array_data: None,
            list_array_data: Some(Box::new(ListArrayData {
                offsets: self.offsets.iter().map(|u| *u as u32).collect(),
                value: Some(Box::new(value)),
                value_type: Some(self.value_type.to_protobuf()),
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

    fn hash_at<H: std::hash::Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.value.hash_at(idx, state)
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> ArrayResult<super::ArrayBuilderImpl> {
        let array_builder = ListArrayBuilder::with_meta(
            capacity,
            ArrayMeta::List {
                datatype: Box::new(self.value_type.clone()),
            },
        );
        Ok(ArrayBuilderImpl::List(array_builder))
    }

    fn array_meta(&self) -> ArrayMeta {
        ArrayMeta::List {
            datatype: Box::new(self.value_type.clone()),
        }
    }
}

impl ListArray {
    pub fn from_protobuf(array: &ProstArray) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.values.is_empty(),
            "Must have no buffer in a list array"
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        let cardinality = bitmap.len();
        let array_data = array.get_list_array_data()?.to_owned();
        let value = ArrayImpl::from_protobuf(array_data.value.as_ref().unwrap(), cardinality)?;
        let arr = ListArray {
            bitmap,
            offsets: array_data.offsets.iter().map(|u| *u as usize).collect(),
            value: Box::new(value),
            value_type: DataType::from(&array_data.value_type.unwrap()),
            len: cardinality,
        };
        Ok(arr.into())
    }

    // Used for testing purposes
    pub fn from_slices(
        null_bitmap: &[bool],
        values: Vec<Option<ArrayImpl>>,
        value_type: DataType,
    ) -> ArrayResult<ListArray> {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::from_iter(null_bitmap.to_vec());
        let mut offsets = vec![0];
        let mut values = values.into_iter().peekable();
        let mut builder = values.peek().unwrap().as_ref().unwrap().create_builder(0)?;
        values.try_for_each(|i| match i {
            Some(a) => {
                offsets.push(a.len());
                builder.append_array(&a)
            }
            None => {
                offsets.push(0);
                Ok(())
            }
        })?;
        offsets.iter_mut().fold(0, |acc, x| {
            *x += acc;
            *x
        });
        Ok(ListArray {
            bitmap,
            offsets,
            value: Box::new(builder.finish()),
            value_type,
            len: cardinality,
        })
    }

    #[cfg(test)]
    pub fn values_vec(&self) -> Vec<Option<ListValue>> {
        use crate::types::ScalarRef;

        self.iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec()
    }
}

#[derive(Clone, Debug, Eq, Default, PartialEq, Hash)]
pub struct ListValue {
    values: Box<[Datum]>,
}

impl fmt::Display for ListValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Example of ListValue display: ARRAY[1, 2]
        write!(
            f,
            "ARRAY[{}]",
            self.values.iter().map(|v| v.as_ref().unwrap()).format(", ")
        )
    }
}

impl PartialOrd for ListValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_scalar_ref().partial_cmp(&other.as_scalar_ref())
    }
}

impl Ord for ListValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl ListValue {
    pub fn new(values: Vec<Datum>) -> Self {
        Self {
            values: values.into_boxed_slice(),
        }
    }

    pub fn values(&self) -> &[Datum] {
        &self.values
    }

    pub fn to_protobuf_owned(&self) -> Vec<u8> {
        self.as_scalar_ref().to_protobuf_owned()
    }

    pub fn from_protobuf_bytes(data_type: ProstDataType, b: &Vec<u8>) -> ArrayResult<Self> {
        let list_value: ProstListValue = Message::decode(b.as_slice())?;
        let d = &data_type.field_type[0];
        let fields: Vec<Datum> = list_value
            .fields
            .iter()
            .map(|b| {
                if b.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(ScalarImpl::from_proto_bytes(b, d)?))
                }
            })
            .collect::<ArrayResult<Vec<Datum>>>()?;
        Ok(ListValue::new(fields))
    }

    pub fn deserialize(
        datatype: &DataType,
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        // This is a bit dirty, but idk how to correctly deserialize bytes in memcomparable
        // format without this...
        struct Visitor<'a>(&'a DataType);
        impl<'a> serde::de::Visitor<'a> for Visitor<'a> {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a list of {}", self.0)
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(v)
            }
        }
        let visitor = Visitor(datatype);
        let bytes = deserializer.deserialize_byte_buf(visitor)?;
        let mut inner_deserializer = memcomparable::Deserializer::new(bytes.as_slice());
        let mut values = Vec::new();
        while inner_deserializer.has_remaining() {
            values.push(deserialize_datum_from(datatype, &mut inner_deserializer)?)
        }
        Ok(Self::new(values))
    }
}

#[derive(Copy, Clone)]
pub enum ListRef<'a> {
    Indexed { arr: &'a ListArray, idx: usize },
    ValueRef { val: &'a ListValue },
}

macro_rules! iter_elems_ref {
    ($self:ident, $it:ident, { $($body:tt)* }) => {
        match $self {
            ListRef::Indexed { arr, idx } => {
                let $it = (arr.offsets[*idx]..arr.offsets[*idx + 1]).map(|o| arr.value.value_at(o));
                $($body)*
            }
            ListRef::ValueRef { val } => {
                let $it = val.values.iter().map(to_datum_ref);
                $($body)*
            }
        }
    };
}

macro_rules! iter_elems {
    ($self:ident, $it:ident, { $($body:tt)* }) => {
        match $self {
            ListRef::Indexed { arr, idx } => {
                let $it = (arr.offsets[*idx]..arr.offsets[*idx + 1]).map(|o| arr.value.value_at(o).to_owned_datum());
                $($body)*
            }
            ListRef::ValueRef { val } => {
                let $it = val.values.iter();
                $($body)*
            }
        }
    };
}

impl<'a> ListRef<'a> {
    pub fn flatten(&self) -> Vec<DatumRef<'a>> {
        iter_elems_ref!(self, it, {
            it.flat_map(|datum_ref| {
                if let Some(ScalarRefImpl::List(list_ref)) = datum_ref {
                    list_ref.flatten()
                } else {
                    vec![datum_ref]
                }
                .into_iter()
            })
            .collect()
        })
    }

    pub fn values_ref(&self) -> Vec<DatumRef<'a>> {
        iter_elems_ref!(self, it, { it.collect() })
    }

    pub fn value_at(&self, index: usize) -> ArrayResult<DatumRef<'a>> {
        match self {
            ListRef::Indexed { arr, idx } => {
                if index <= arr.value.len() {
                    Ok(arr.value.value_at(arr.offsets[*idx] + index - 1))
                } else {
                    Ok(None)
                }
            }
            ListRef::ValueRef { val } => {
                if let Some(datum) = val.values().iter().nth(index - 1) {
                    Ok(to_datum_ref(datum))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn to_protobuf_owned(&self) -> Vec<u8> {
        let elems = iter_elems!(self, it, {
            it.map(|f| match f {
                None => {
                    vec![]
                }
                Some(s) => s.to_protobuf(),
            })
            .collect_vec()
        });
        ProstListValue { fields: elems }.encode_to_vec()
    }

    pub fn serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        let mut inner_serializer = memcomparable::Serializer::new(vec![]);
        iter_elems_ref!(self, it, {
            for datum_ref in it {
                serialize_datum_ref_into(&datum_ref, &mut inner_serializer)?
            }
        });
        serializer.serialize_bytes(&inner_serializer.into_inner())
    }
}

impl Hash for ListRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ListRef::Indexed { arr, idx } => arr.hash_at(*idx, state),
            ListRef::ValueRef { val } => val.hash(state),
        }
    }
}

impl PartialEq for ListRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.values_ref().eq(&other.values_ref())
    }
}

impl PartialOrd for ListRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let l = self.values_ref();
        let r = other.values_ref();
        let it = l.iter().zip_longest(r.iter()).find_map(|e| match e {
            Both(ls, rs) => {
                let ord = cmp_list_value(ls, rs);
                if let Ordering::Equal = ord {
                    None
                } else {
                    Some(ord)
                }
            }
            Left(_) => Some(Ordering::Greater),
            Right(_) => Some(Ordering::Less),
        });
        it.or(Some(Ordering::Equal))
    }
}

fn cmp_list_value(l: &Option<ScalarRefImpl>, r: &Option<ScalarRefImpl>) -> Ordering {
    match (l, r) {
        // Comparability check was performed by frontend beforehand.
        (Some(sl), Some(sr)) => sl.partial_cmp(sr).unwrap(),
        // Nulls are larger than everything, ARRAY[1, null] > ARRAY[1, 2] for example.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl Debug for ListRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        iter_elems_ref!(self, it, {
            for v in it {
                v.fmt(f)?;
            }
            Ok(())
        })
    }
}

impl Display for ListRef<'_> {
    // This function will be invoked when pgwire prints a list value in string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        iter_elems_ref!(self, it, {
            write!(f, "{{{}}}", it.map(display_datum_ref).join(","))
        })
    }
}

impl Eq for ListRef<'_> {}

impl Ord for ListRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order between two lists is deterministic.
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use more_asserts::{assert_gt, assert_lt};

    use super::*;
    use crate::{array, empty_array, try_match_expand};

    #[test]
    fn test_list_with_values() {
        use crate::array::*;
        let arr = ListArray::from_slices(
            &[true, false, true, true],
            vec![
                Some(array! { I32Array, [Some(12), Some(-7), Some(25)] }.into()),
                None,
                Some(array! { I32Array, [Some(0), Some(-127), Some(127), Some(50)] }.into()),
                Some(empty_array! { I32Array }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();
        let actual = ListArray::from_protobuf(&arr.to_protobuf()).unwrap();
        let tmp = ArrayImpl::List(arr);
        assert_eq!(tmp, actual);

        let arr = try_match_expand!(actual, ArrayImpl::List).unwrap();
        let list_values = arr.values_vec();
        assert_eq!(
            list_values,
            vec![
                Some(ListValue::new(vec![
                    Some(ScalarImpl::Int32(12)),
                    Some(ScalarImpl::Int32(-7)),
                    Some(ScalarImpl::Int32(25)),
                ])),
                None,
                Some(ListValue::new(vec![
                    Some(ScalarImpl::Int32(0)),
                    Some(ScalarImpl::Int32(-127)),
                    Some(ScalarImpl::Int32(127)),
                    Some(ScalarImpl::Int32(50)),
                ])),
                Some(ListValue::new(vec![])),
            ]
        );

        let mut builder = ListArrayBuilder::with_meta(
            4,
            ArrayMeta::List {
                datatype: Box::new(DataType::Int32),
            },
        );
        list_values.iter().for_each(|v| {
            builder
                .append(v.as_ref().map(|s| s.as_scalar_ref()))
                .unwrap()
        });
        let arr = builder.finish();
        assert_eq!(arr.values_vec(), list_values);

        let part1 = ListArray::from_slices(
            &[true, false],
            vec![
                Some(array! { I32Array, [Some(12), Some(-7), Some(25)] }.into()),
                None,
            ],
            DataType::Int32,
        )
        .unwrap();

        let part2 = ListArray::from_slices(
            &[true, true],
            vec![
                Some(array! { I32Array, [Some(0), Some(-127), Some(127), Some(50)] }.into()),
                Some(empty_array! { I32Array }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();

        let mut builder = ListArrayBuilder::with_meta(
            4,
            ArrayMeta::List {
                datatype: Box::new(DataType::Int32),
            },
        );
        builder.append_array(&part1).unwrap();
        builder.append_array(&part2).unwrap();

        assert_eq!(arr.values_vec(), builder.finish().values_vec());
    }

    // Ensure `create_builder` exactly copies the same metadata.
    #[test]
    fn test_list_create_builder() {
        use crate::array::*;
        let arr = ListArray::from_slices(
            &[true],
            vec![Some(
                array! { F32Array, [Some(2.0), Some(42.0), Some(1.0)] }.into(),
            )],
            DataType::Float32,
        )
        .unwrap();
        let builder = arr.create_builder(0).unwrap();
        let arr2 = try_match_expand!(builder.finish(), ArrayImpl::List).unwrap();
        assert_eq!(arr.array_meta(), arr2.array_meta());
    }

    #[test]
    fn test_list_nested_layout() {
        use crate::array::*;

        let listarray1 = ListArray::from_slices(
            &[true, true],
            vec![
                Some(array! { I32Array, [Some(1), Some(2)] }.into()),
                Some(array! { I32Array, [Some(3), Some(4)] }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();

        let listarray2 = ListArray::from_slices(
            &[true, false, true],
            vec![
                Some(array! { I32Array, [Some(5), Some(6), Some(7)] }.into()),
                None,
                Some(array! { I32Array, [Some(8)] }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();

        let listarray3 = ListArray::from_slices(
            &[true],
            vec![Some(array! { I32Array, [Some(9), Some(10)] }.into())],
            DataType::Int32,
        )
        .unwrap();

        let nestarray = ListArray::from_slices(
            &[true, true, true],
            vec![
                Some(listarray1.into()),
                Some(listarray2.into()),
                Some(listarray3.into()),
            ],
            DataType::List {
                datatype: Box::new(DataType::Int32),
            },
        )
        .unwrap();
        let actual = ListArray::from_protobuf(&nestarray.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::List(nestarray), actual);

        let nestarray = try_match_expand!(actual, ArrayImpl::List).unwrap();
        let nested_list_values = nestarray.values_vec();
        assert_eq!(
            nested_list_values,
            vec![
                Some(ListValue::new(vec![
                    Some(ScalarImpl::List(ListValue::new(vec![
                        Some(ScalarImpl::Int32(1)),
                        Some(ScalarImpl::Int32(2)),
                    ]))),
                    Some(ScalarImpl::List(ListValue::new(vec![
                        Some(ScalarImpl::Int32(3)),
                        Some(ScalarImpl::Int32(4)),
                    ]))),
                ])),
                Some(ListValue::new(vec![
                    Some(ScalarImpl::List(ListValue::new(vec![
                        Some(ScalarImpl::Int32(5)),
                        Some(ScalarImpl::Int32(6)),
                        Some(ScalarImpl::Int32(7)),
                    ]))),
                    None,
                    Some(ScalarImpl::List(ListValue::new(vec![Some(
                        ScalarImpl::Int32(8)
                    ),]))),
                ])),
                Some(ListValue::new(vec![Some(ScalarImpl::List(
                    ListValue::new(vec![
                        Some(ScalarImpl::Int32(9)),
                        Some(ScalarImpl::Int32(10)),
                    ])
                )),])),
            ]
        );

        let mut builder = ListArrayBuilder::with_meta(
            3,
            ArrayMeta::List {
                datatype: Box::new(DataType::List {
                    datatype: Box::new(DataType::Int32),
                }),
            },
        );
        nested_list_values.iter().for_each(|v| {
            builder
                .append(v.as_ref().map(|s| s.as_scalar_ref()))
                .unwrap()
        });
        let nestarray = builder.finish();
        assert_eq!(nestarray.values_vec(), nested_list_values);
    }

    #[test]
    fn test_list_value_cmp() {
        // ARRAY[1, 1] < ARRAY[1, 2, 1]
        assert_lt!(
            ListValue::new(vec![Some(1.into()), Some(1.into())]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // ARRAY[1, 2] < ARRAY[1, 2, 1]
        assert_lt!(
            ListValue::new(vec![Some(1.into()), Some(2.into())]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // ARRAY[1, 3] > ARRAY[1, 2, 1]
        assert_gt!(
            ListValue::new(vec![Some(1.into()), Some(3.into())]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // null > 1
        assert_eq!(
            cmp_list_value(&None, &Some(ScalarRefImpl::Int32(1))),
            Ordering::Greater
        );
        // ARRAY[1, 2, null] > ARRAY[1, 2, 1]
        assert_gt!(
            ListValue::new(vec![Some(1.into()), Some(2.into()), None]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // Null value in first ARRAY results into a Greater ordering regardless of the smaller ARRAY
        // length. ARRAY[1, null] > ARRAY[1, 2, 3]
        assert_gt!(
            ListValue::new(vec![Some(1.into()), None]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(3.into())]),
        );
        // ARRAY[1, null] == ARRAY[1, null]
        assert_eq!(
            ListValue::new(vec![Some(1.into()), None]),
            ListValue::new(vec![Some(1.into()), None]),
        );
    }

    #[test]
    fn test_list_ref_display() {
        let v = ListValue::new(vec![Some(1.into()), None]);
        let r = ListRef::ValueRef { val: &v };
        assert_eq!("{1,NULL}".to_string(), format!("{}", r));
    }

    #[test]
    fn test_to_protobuf_owned() {
        use crate::array::*;
        let arr = ListArray::from_slices(
            &[true, true],
            vec![
                Some(array! { I32Array, [Some(1), Some(2)] }.into()),
                Some(array! { I32Array, [Some(3), Some(4)] }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();
        let list_ref = arr.value_at(0).unwrap();
        let output = list_ref.to_protobuf_owned();
        let expect = ListValue::new(vec![
            Some(1i32.to_scalar_value()),
            Some(2i32.to_scalar_value()),
        ])
        .to_protobuf_owned();
        assert_eq!(output, expect);

        let list_ref = arr.value_at(1).unwrap();
        let output = list_ref.to_protobuf_owned();
        let expect = ListValue::new(vec![
            Some(3i32.to_scalar_value()),
            Some(4i32.to_scalar_value()),
        ])
        .to_protobuf_owned();
        assert_eq!(output, expect);
    }

    #[test]
    fn test_serialize_deserialize() {
        let value = ListValue::new(vec![
            Some("abcd".to_string().to_scalar_value()),
            Some("".to_string().to_scalar_value()),
            None,
            Some("a".to_string().to_scalar_value()),
        ]);
        let list_ref = ListRef::ValueRef { val: &value };
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serializer.set_reverse(true);
        list_ref.serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        deserializer.set_reverse(true);
        assert_eq!(
            ListValue::deserialize(&DataType::Varchar, &mut deserializer).unwrap(),
            value
        );

        let mut builder = ListArrayBuilder::with_meta(
            0,
            ArrayMeta::List {
                datatype: Box::new(DataType::Varchar),
            },
        );
        builder.append(Some(list_ref));
        let array = builder.finish();
        let list_ref = array.value_at(0).unwrap();
        let mut serializer = memcomparable::Serializer::new(vec![]);
        list_ref.serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            ListValue::deserialize(&DataType::Varchar, &mut deserializer).unwrap(),
            value
        );
    }

    #[test]
    fn test_memcomparable() {
        let cases = [
            (
                ListValue::new(vec![
                    Some(123.to_scalar_value()),
                    Some(456.to_scalar_value()),
                ]),
                ListValue::new(vec![
                    Some(123.to_scalar_value()),
                    Some(789.to_scalar_value()),
                ]),
                DataType::Int32,
                Ordering::Less,
            ),
            (
                ListValue::new(vec![
                    Some(123.to_scalar_value()),
                    Some(456.to_scalar_value()),
                ]),
                ListValue::new(vec![Some(123.to_scalar_value())]),
                DataType::Int32,
                Ordering::Greater,
            ),
            (
                ListValue::new(vec![None, Some("".to_string().to_scalar_value())]),
                ListValue::new(vec![None, None]),
                DataType::Varchar,
                Ordering::Less,
            ),
            (
                ListValue::new(vec![Some(2.to_scalar_value())]),
                ListValue::new(vec![
                    Some(1.to_scalar_value()),
                    None,
                    Some(3.to_scalar_value()),
                ]),
                DataType::Int32,
                Ordering::Greater,
            ),
        ];

        for (lhs, rhs, datatype, order) in cases {
            let lhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                ListRef::ValueRef { val: &lhs }
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                ListRef::ValueRef { val: &rhs }
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), order);

            let mut builder = ListArrayBuilder::with_meta(
                0,
                ArrayMeta::List {
                    datatype: Box::new(datatype),
                },
            );
            builder
                .append(Some(ListRef::ValueRef { val: &lhs }))
                .unwrap();
            builder
                .append(Some(ListRef::ValueRef { val: &rhs }))
                .unwrap();
            let array = builder.finish();
            let lhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                array
                    .value_at(0)
                    .unwrap()
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                array
                    .value_at(1)
                    .unwrap()
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), order);
        }
    }

    #[test]
    fn test_listref() {
        use crate::array::*;
        use crate::types;
        let arr = ListArray::from_slices(
            &[true, false, true],
            vec![
                Some(array! { I32Array, [Some(1), Some(2), Some(3)] }.into()),
                None,
                Some(array! { I32Array, [Some(4), Some(5), Some(6), Some(7)] }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();

        // get 3rd ListRef from ListArray
        let list_ref = arr.value_at(2).unwrap();
        assert_eq!(
            list_ref,
            ListRef::ValueRef {
                val: &ListValue::new(vec![
                    Some(4.to_scalar_value()),
                    Some(5.to_scalar_value()),
                    Some(6.to_scalar_value()),
                    Some(7.to_scalar_value()),
                ]),
            }
        );

        // Get 2nd value from ListRef
        let scalar = list_ref.value_at(2).unwrap();
        assert_eq!(scalar, Some(types::ScalarRefImpl::Int32(5)));
    }
}
