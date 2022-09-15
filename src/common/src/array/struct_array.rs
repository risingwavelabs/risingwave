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
use std::sync::Arc;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use prost::Message;
use risingwave_pb::data::{
    Array as ProstArray, ArrayType as ProstArrayType, DataType as ProstDataType, StructArrayData,
};
use risingwave_pb::expr::StructValue as ProstStructValue;

use super::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayIterator, ArrayMeta, ArrayResult,
    NULL_VAL_FOR_HASH,
};
use crate::array::ArrayRef;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::types::{
    deserialize_datum_from, display_datum_ref, serialize_datum_ref_into, to_datum_ref, DataType,
    Datum, DatumRef, Scalar, ScalarImpl, ScalarRefImpl, ToOwnedDatum,
};

#[derive(Debug)]
pub struct StructArrayBuilder {
    bitmap: BitmapBuilder,
    children_array: Vec<ArrayBuilderImpl>,
    children_type: Arc<[DataType]>,
    len: usize,
}

impl ArrayBuilder for StructArrayBuilder {
    type ArrayType = StructArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Self {
        panic!("Must use with_meta.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_meta(
            capacity,
            ArrayMeta::Struct {
                children: Arc::new([]),
            },
        )
    }

    fn with_meta(capacity: usize, meta: ArrayMeta) -> Self {
        if let ArrayMeta::Struct { children } = meta {
            let children_array = children
                .iter()
                .map(|a| a.create_array_builder(capacity))
                .collect();
            Self {
                bitmap: BitmapBuilder::with_capacity(capacity),
                children_array,
                children_type: children,
                len: 0,
            }
        } else {
            panic!("must be ArrayMeta::Struct");
        }
    }

    fn append(&mut self, value: Option<StructRef<'_>>) -> ArrayResult<()> {
        match value {
            None => {
                self.bitmap.append(false);
                for child in &mut self.children_array {
                    child.append_datum_ref(None)?;
                }
            }
            Some(v) => {
                self.bitmap.append(true);
                let fields = v.fields_ref();
                assert_eq!(fields.len(), self.children_array.len());
                for (field_idx, f) in fields.into_iter().enumerate() {
                    self.children_array[field_idx].append_datum_ref(f)?;
                }
            }
        }
        self.len += 1;
        Ok(())
    }

    fn append_array(&mut self, other: &StructArray) -> ArrayResult<()> {
        self.bitmap.append_bitmap(&other.bitmap);
        self.children_array
            .iter_mut()
            .enumerate()
            .try_for_each(|(i, a)| a.append_array(&other.children[i]))?;
        self.len += other.len();
        Ok(())
    }

    fn pop(&mut self) -> Option<()> {
        if self.bitmap.pop().is_some() {
            for child in &mut self.children_array {
                child.pop().unwrap()
            }
            self.len -= 1;

            Some(())
        } else {
            None
        }
    }

    fn finish(self) -> StructArray {
        let children = self
            .children_array
            .into_iter()
            .map(|b| Arc::new(b.finish()))
            .collect::<Vec<ArrayRef>>();
        StructArray {
            bitmap: self.bitmap.finish(),
            children,
            children_type: self.children_type,
            len: self.len,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StructArray {
    bitmap: Bitmap,
    children: Vec<ArrayRef>,
    children_type: Arc<[DataType]>,
    len: usize,
}

impl StructArrayBuilder {
    pub fn append_array_refs(&mut self, refs: Vec<ArrayRef>, len: usize) -> ArrayResult<()> {
        for _ in 0..len {
            self.bitmap.append(true);
        }
        self.len += len;
        self.children_array
            .iter_mut()
            .zip_eq(refs.iter())
            .try_for_each(|(a, r)| a.append_array(r))
    }
}

impl Array for StructArray {
    type Builder = StructArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = StructValue;
    type RefItem<'a> = StructRef<'a>;

    fn value_at(&self, idx: usize) -> Option<StructRef<'_>> {
        if !self.is_null(idx) {
            Some(StructRef::Indexed { arr: self, idx })
        } else {
            None
        }
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<StructRef<'_>> {
        if !self.is_null_unchecked(idx) {
            Some(StructRef::Indexed { arr: self, idx })
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
        let children_array = self.children.iter().map(|a| a.to_protobuf()).collect();
        let children_type = self.children_type.iter().map(|t| t.to_protobuf()).collect();
        ProstArray {
            array_type: ProstArrayType::Struct as i32,
            struct_array_data: Some(StructArrayData {
                children_array,
                children_type,
            }),
            list_array_data: None,
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
            self.children.iter().for_each(|a| a.hash_at(idx, state))
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> ArrayResult<super::ArrayBuilderImpl> {
        let array_builder = StructArrayBuilder::with_meta(
            capacity,
            ArrayMeta::Struct {
                children: self.children_type.clone(),
            },
        );
        Ok(ArrayBuilderImpl::Struct(array_builder))
    }

    fn array_meta(&self) -> ArrayMeta {
        ArrayMeta::Struct {
            children: self.children_type.clone(),
        }
    }
}

impl StructArray {
    pub fn from_protobuf(array: &ProstArray) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.values.is_empty(),
            "Must have no buffer in a struct array"
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        let cardinality = bitmap.len();
        let array_data = array.get_struct_array_data()?;
        let children = array_data
            .children_array
            .iter()
            .map(|child| Ok(Arc::new(ArrayImpl::from_protobuf(child, cardinality)?)))
            .collect::<ArrayResult<Vec<ArrayRef>>>()?;
        let children_type: Arc<[DataType]> = array_data
            .children_type
            .iter()
            .map(DataType::from)
            .collect::<Vec<DataType>>()
            .into();
        let arr = StructArray {
            bitmap,
            children,
            children_type,
            len: cardinality,
        };
        Ok(arr.into())
    }

    pub fn children_array_types(&self) -> &[DataType] {
        &self.children_type
    }

    pub fn field_at(&self, index: usize) -> ArrayRef {
        self.children[index].clone()
    }

    pub fn from_slices(
        null_bitmap: &[bool],
        children: Vec<ArrayImpl>,
        children_type: Vec<DataType>,
    ) -> ArrayResult<StructArray> {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::from_iter(null_bitmap.to_vec());
        let children = children.into_iter().map(Arc::new).collect_vec();
        Ok(StructArray {
            bitmap,
            children_type: children_type.into(),
            len: cardinality,
            children,
        })
    }

    #[cfg(test)]
    pub fn values_vec(&self) -> Vec<Option<StructValue>> {
        use crate::types::ScalarRef;

        self.iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec()
    }
}

#[derive(Clone, Debug, Eq, Default)]
pub struct StructValue {
    fields: Box<[Datum]>,
}

impl fmt::Display for StructValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({})",
            self.fields
                .iter()
                .map(|f| {
                    match f {
                        Some(f) => format!("{}", f),
                        None => " ".to_string(),
                    }
                })
                .join(", ")
        )
    }
}

impl PartialEq for StructValue {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl PartialOrd for StructValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_scalar_ref().partial_cmp(&other.as_scalar_ref())
    }
}

impl Ord for StructValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Hash for StructValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

impl StructValue {
    pub fn new(fields: Vec<Datum>) -> Self {
        Self {
            fields: fields.into_boxed_slice(),
        }
    }

    pub fn fields(&self) -> &[Datum] {
        &self.fields
    }

    pub fn to_protobuf_owned(&self) -> Vec<u8> {
        self.as_scalar_ref().to_protobuf_owned()
    }

    pub fn from_protobuf_bytes(data_type: ProstDataType, b: &Vec<u8>) -> ArrayResult<Self> {
        let struct_value: ProstStructValue = Message::decode(b.as_slice())?;
        let fields: Vec<Datum> = struct_value
            .fields
            .iter()
            .zip_eq(data_type.field_type.iter())
            .map(|(b, d)| {
                if b.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(ScalarImpl::from_proto_bytes(b, d)?))
                }
            })
            .collect::<ArrayResult<Vec<Datum>>>()?;
        Ok(StructValue::new(fields))
    }

    pub fn deserialize(
        fields: &[DataType],
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        fields
            .iter()
            .map(|field| deserialize_datum_from(field, deserializer))
            .try_collect()
            .map(Self::new)
    }
}

#[derive(Copy, Clone)]
pub enum StructRef<'a> {
    Indexed { arr: &'a StructArray, idx: usize },
    ValueRef { val: &'a StructValue },
}

macro_rules! iter_fields_ref {
    ($self:ident, $it:ident, { $($body:tt)* }) => {
        match $self {
            StructRef::Indexed { arr, idx } => {
                let $it = arr.children.iter().map(|a| a.value_at(*idx));
                $($body)*
            }
            StructRef::ValueRef { val } => {
                let $it = val.fields.iter().map(to_datum_ref);
                $($body)*
            }
        }
    };
}

macro_rules! iter_fields {
    ($self:ident, $it:ident, { $($body:tt)* }) => {
        match &$self {
            StructRef::Indexed { arr, idx } => {
                let $it = arr
                    .children
                    .iter()
                    .map(|a| a.value_at(*idx).to_owned_datum());
                $($body)*
            }
            StructRef::ValueRef { val } => {
                let $it = val.fields.iter();
                $($body)*
            }
        }
    };
}

impl<'a> StructRef<'a> {
    pub fn fields_ref(&self) -> Vec<DatumRef<'a>> {
        iter_fields_ref!(self, it, { it.collect() })
    }

    pub fn to_protobuf_owned(&self) -> Vec<u8> {
        let fields = iter_fields!(self, it, {
            it.map(|f| match f {
                None => {
                    vec![]
                }
                Some(s) => s.to_protobuf(),
            })
            .collect_vec()
        });
        ProstStructValue { fields }.encode_to_vec()
    }

    pub fn serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        iter_fields_ref!(self, it, {
            for datum_ref in it {
                serialize_datum_ref_into(&datum_ref, serializer)?
            }
            Ok(())
        })
    }
}

impl Hash for StructRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            StructRef::Indexed { arr, idx } => arr.hash_at(*idx, state),
            StructRef::ValueRef { val } => val.hash(state),
        }
    }
}

impl PartialEq for StructRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.fields_ref().eq(&other.fields_ref())
    }
}

impl PartialOrd for StructRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let l = self.fields_ref();
        let r = other.fields_ref();
        debug_assert_eq!(
            l.len(),
            r.len(),
            "Structs must have the same length to be compared"
        );
        let it = l.iter().enumerate().find_map(|(idx, v)| {
            let ord = cmp_struct_field(v, &r[idx]);
            if let Ordering::Equal = ord {
                None
            } else {
                Some(ord)
            }
        });
        it.or(Some(Ordering::Equal))
    }
}

fn cmp_struct_field(l: &Option<ScalarRefImpl>, r: &Option<ScalarRefImpl>) -> Ordering {
    match (l, r) {
        // Comparability check was performed by frontend beforehand.
        (Some(sl), Some(sr)) => sl.partial_cmp(sr).unwrap(),
        // Nulls are larger than everything, (1, null) > (1, 2) for example.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl Debug for StructRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        iter_fields_ref!(self, it, {
            for v in it {
                v.fmt(f)?;
            }
            Ok(())
        })
    }
}

impl Display for StructRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        iter_fields_ref!(self, it, {
            write!(f, "({})", it.map(display_datum_ref).join(","))
        })
    }
}

impl Eq for StructRef<'_> {}

impl Ord for StructRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order between two structs is deterministic.
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use more_asserts::assert_gt;

    use super::*;
    use crate::types::{OrderedF32, OrderedF64};
    use crate::{array, try_match_expand};

    // Empty struct is allowed in postgres.
    // `CREATE TYPE foo_empty as ();`, e.g.
    #[test]
    fn test_struct_new_empty() {
        let arr = StructArray::from_slices(&[true, false, true, false], vec![], vec![]).unwrap();
        let actual = StructArray::from_protobuf(&arr.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);
    }

    #[test]
    fn test_struct_with_fields() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[false, true, false, true],
            vec![
                array! { I32Array, [None, Some(1), None, Some(2)] }.into(),
                array! { F32Array, [None, Some(3.0), None, Some(4.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap();
        let actual = StructArray::from_protobuf(&arr.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);

        let arr = try_match_expand!(actual, ArrayImpl::Struct).unwrap();
        let struct_values = arr.values_vec();
        assert_eq!(
            struct_values,
            vec![
                None,
                Some(StructValue::new(vec![
                    Some(ScalarImpl::Int32(1)),
                    Some(ScalarImpl::Float32(3.0.into())),
                ])),
                None,
                Some(StructValue::new(vec![
                    Some(ScalarImpl::Int32(2)),
                    Some(ScalarImpl::Float32(4.0.into())),
                ])),
            ]
        );

        let mut builder = StructArrayBuilder::with_meta(
            4,
            ArrayMeta::Struct {
                children: Arc::new([DataType::Int32, DataType::Float32]),
            },
        );
        struct_values.iter().for_each(|v| {
            builder
                .append(v.as_ref().map(|s| s.as_scalar_ref()))
                .unwrap()
        });
        let arr = builder.finish();
        assert_eq!(arr.values_vec(), struct_values);
    }

    // Ensure `create_builder` exactly copies the same metadata.
    #[test]
    fn test_struct_create_builder() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1)] }.into(),
                array! { F32Array, [Some(2.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap();
        let builder = arr.create_builder(4).unwrap();
        let arr2 = try_match_expand!(builder.finish(), ArrayImpl::Struct).unwrap();
        assert_eq!(arr.array_meta(), arr2.array_meta());
    }

    #[test]
    fn test_struct_value_cmp() {
        // (1, 2.0) > (1, 1.0)
        assert_gt!(
            StructValue::new(vec![Some(1.into()), Some(2.0.into())]),
            StructValue::new(vec![Some(1.into()), Some(1.0.into())]),
        );
        // null > 1
        assert_eq!(
            cmp_struct_field(&None, &Some(ScalarRefImpl::Int32(1))),
            Ordering::Greater
        );
        // (1, null, 3) > (1, 1.0, 2)
        assert_gt!(
            StructValue::new(vec![Some(1.into()), None, Some(3.into())]),
            StructValue::new(vec![Some(1.into()), Some(1.0.into()), Some(2.into())]),
        );
        // (1, null) == (1, null)
        assert_eq!(
            StructValue::new(vec![Some(1.into()), None]),
            StructValue::new(vec![Some(1.into()), None]),
        );
    }

    #[test]
    fn test_to_protobuf_owned() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1)] }.into(),
                array! { F32Array, [Some(2.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap();
        let struct_ref = arr.value_at(0).unwrap();
        let output = struct_ref.to_protobuf_owned();
        let expect = StructValue::new(vec![
            Some(1i32.to_scalar_value()),
            Some(OrderedF32::from(2.0f32).to_scalar_value()),
        ])
        .to_protobuf_owned();
        assert_eq!(output, expect);
    }

    #[test]
    fn test_serialize_deserialize() {
        let value = StructValue::new(vec![
            Some(OrderedF32::from(3.2).to_scalar_value()),
            Some("abcde".to_string().to_scalar_value()),
            Some(
                StructValue::new(vec![
                    Some(OrderedF64::from(1.3).to_scalar_value()),
                    Some("a".to_string().to_scalar_value()),
                    None,
                    Some(StructValue::new(vec![]).to_scalar_value()),
                ])
                .to_scalar_value(),
            ),
            None,
            Some("".to_string().to_scalar_value()),
            None,
            Some(StructValue::new(vec![]).to_scalar_value()),
            Some(12345.to_scalar_value()),
        ]);
        let fields = [
            DataType::Float32,
            DataType::Varchar,
            DataType::new_struct(
                vec![
                    DataType::Float64,
                    DataType::Varchar,
                    DataType::Varchar,
                    DataType::new_struct(vec![], vec![]),
                ],
                vec![],
            ),
            DataType::Int64,
            DataType::Varchar,
            DataType::Int16,
            DataType::new_struct(vec![], vec![]),
            DataType::Int32,
        ];
        let struct_ref = StructRef::ValueRef { val: &value };
        let mut serializer = memcomparable::Serializer::new(vec![]);
        struct_ref.serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            StructValue::deserialize(&fields, &mut deserializer).unwrap(),
            value
        );

        let mut builder = StructArrayBuilder::with_meta(
            0,
            ArrayMeta::Struct {
                children: Arc::new(fields.clone()),
            },
        );
        builder.append(Some(struct_ref)).unwrap();
        let array = builder.finish();
        let struct_ref = array.value_at(0).unwrap();
        let mut serializer = memcomparable::Serializer::new(vec![]);
        struct_ref.serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            StructValue::deserialize(&fields, &mut deserializer).unwrap(),
            value
        );
    }

    #[test]
    fn test_memcomparable() {
        let cases = [
            (
                StructValue::new(vec![
                    Some(123.to_scalar_value()),
                    Some(456i64.to_scalar_value()),
                ]),
                StructValue::new(vec![
                    Some(123.to_scalar_value()),
                    Some(789i64.to_scalar_value()),
                ]),
                vec![DataType::Int32, DataType::Int64],
                Ordering::Less,
            ),
            (
                StructValue::new(vec![
                    Some(123.to_scalar_value()),
                    Some(456i64.to_scalar_value()),
                ]),
                StructValue::new(vec![
                    Some(1.to_scalar_value()),
                    Some(789i64.to_scalar_value()),
                ]),
                vec![DataType::Int32, DataType::Int64],
                Ordering::Greater,
            ),
            (
                StructValue::new(vec![Some("".to_string().to_scalar_value())]),
                StructValue::new(vec![None]),
                vec![DataType::Varchar],
                Ordering::Less,
            ),
            (
                StructValue::new(vec![Some("abcd".to_string().to_scalar_value()), None]),
                StructValue::new(vec![
                    Some("abcd".to_string().to_scalar_value()),
                    Some(
                        StructValue::new(vec![Some("abcdef".to_string().to_scalar_value())])
                            .to_scalar_value(),
                    ),
                ]),
                vec![
                    DataType::Varchar,
                    DataType::new_struct(vec![DataType::Varchar], vec![]),
                ],
                Ordering::Greater,
            ),
            (
                StructValue::new(vec![
                    Some("abcd".to_string().to_scalar_value()),
                    Some(
                        StructValue::new(vec![Some("abcdef".to_string().to_scalar_value())])
                            .to_scalar_value(),
                    ),
                ]),
                StructValue::new(vec![
                    Some("abcd".to_string().to_scalar_value()),
                    Some(
                        StructValue::new(vec![Some("abcdef".to_string().to_scalar_value())])
                            .to_scalar_value(),
                    ),
                ]),
                vec![
                    DataType::Varchar,
                    DataType::new_struct(vec![DataType::Varchar], vec![]),
                ],
                Ordering::Equal,
            ),
        ];

        for (lhs, rhs, fields, order) in cases {
            let lhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                StructRef::ValueRef { val: &lhs }
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                StructRef::ValueRef { val: &rhs }
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), order);

            let mut builder = StructArrayBuilder::with_meta(
                0,
                ArrayMeta::Struct {
                    children: Arc::from(fields),
                },
            );
            builder
                .append(Some(StructRef::ValueRef { val: &lhs }))
                .unwrap();
            builder
                .append(Some(StructRef::ValueRef { val: &rhs }))
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
}
