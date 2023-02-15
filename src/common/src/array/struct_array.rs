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

use core::fmt;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_pb::data::{Array as ProstArray, ArrayType as ProstArrayType, StructArrayData};

use super::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayMeta, ArrayResult};
use crate::array::ArrayRef;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::types::to_text::ToText;
use crate::types::{
    hash_datum, memcmp_deserialize_datum_from, memcmp_serialize_datum_into, DataType, Datum,
    DatumRef, Scalar, ScalarRefImpl, ToDatumRef,
};
use crate::util::iter_util::ZipEqFast;

#[derive(Debug)]
pub struct StructArrayBuilder {
    bitmap: BitmapBuilder,
    pub(super) children_array: Vec<ArrayBuilderImpl>,
    children_type: Arc<[DataType]>,
    children_names: Arc<[String]>,
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
                children_names: Arc::new([]),
            },
        )
    }

    fn with_meta(capacity: usize, meta: ArrayMeta) -> Self {
        if let ArrayMeta::Struct {
            children,
            children_names,
        } = meta
        {
            let children_array = children
                .iter()
                .map(|a| a.create_array_builder(capacity))
                .collect();
            Self {
                bitmap: BitmapBuilder::with_capacity(capacity),
                children_array,
                children_type: children,
                children_names,
                len: 0,
            }
        } else {
            panic!("must be ArrayMeta::Struct");
        }
    }

    fn append_n(&mut self, n: usize, value: Option<StructRef<'_>>) {
        match value {
            None => {
                self.bitmap.append_n(n, false);
                for child in &mut self.children_array {
                    child.append_datum_n(n, Datum::None);
                }
            }
            Some(v) => {
                self.bitmap.append_n(n, true);
                let fields = v.fields_ref();
                assert_eq!(fields.len(), self.children_array.len());
                for (child, f) in self.children_array.iter_mut().zip_eq_fast(fields) {
                    child.append_datum_n(n, f);
                }
            }
        }
        self.len += n;
    }

    fn append_array(&mut self, other: &StructArray) {
        self.bitmap.append_bitmap(&other.bitmap);
        for (i, a) in self.children_array.iter_mut().enumerate() {
            a.append_array(&other.children[i]);
        }
        self.len += other.len();
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
            children_names: self.children_names,
            len: self.len,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructArray {
    bitmap: Bitmap,
    children: Vec<ArrayRef>,
    children_type: Arc<[DataType]>,
    children_names: Arc<[String]>,
    len: usize,
}

impl StructArrayBuilder {
    pub fn append_array_refs(&mut self, refs: Vec<ArrayRef>, len: usize) {
        for _ in 0..len {
            self.bitmap.append(true);
        }
        self.len += len;
        for (a, r) in self.children_array.iter_mut().zip_eq_fast(refs.iter()) {
            a.append_array(r);
        }
    }
}

impl Array for StructArray {
    type Builder = StructArrayBuilder;
    type OwnedItem = StructValue;
    type RefItem<'a> = StructRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> StructRef<'_> {
        StructRef::Indexed { arr: self, idx }
    }

    fn len(&self) -> usize {
        self.len
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

    fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        let array_builder = StructArrayBuilder::with_meta(
            capacity,
            ArrayMeta::Struct {
                children: self.children_type.clone(),
                children_names: self.children_names.clone(),
            },
        );
        ArrayBuilderImpl::Struct(array_builder)
    }

    fn array_meta(&self) -> ArrayMeta {
        ArrayMeta::Struct {
            children: self.children_type.clone(),
            children_names: self.children_names.clone(),
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
            children_names: vec![].into(),
            len: cardinality,
        };
        Ok(arr.into())
    }

    pub fn children_array_types(&self) -> &[DataType] {
        &self.children_type
    }

    // returns a vector containing a reference to the arrayimpl.
    pub fn field_arrays(&self) -> Vec<&ArrayImpl> {
        self.children.iter().map(|f| &(**f)).collect()
    }

    pub fn field_at(&self, index: usize) -> ArrayRef {
        self.children[index].clone()
    }

    pub fn children_names(&self) -> &[String] {
        &self.children_names
    }

    pub fn from_slices(
        null_bitmap: &[bool],
        children: Vec<ArrayImpl>,
        children_type: Vec<DataType>,
    ) -> StructArray {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::from_iter(null_bitmap.to_vec());
        let children = children.into_iter().map(Arc::new).collect_vec();
        StructArray {
            bitmap,
            children_type: children_type.into(),
            children_names: vec![].into(),
            len: cardinality,
            children,
        }
    }

    pub fn from_slices_with_field_names(
        null_bitmap: &[bool],
        children: Vec<ArrayImpl>,
        children_type: Vec<DataType>,
        children_name: Vec<String>,
    ) -> StructArray {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::from_iter(null_bitmap.to_vec());
        let children = children.into_iter().map(Arc::new).collect_vec();
        StructArray {
            bitmap,
            children_type: children_type.into(),
            children_names: children_name.into(),
            len: cardinality,
            children,
        }
    }

    #[cfg(test)]
    pub fn values_vec(&self) -> Vec<Option<StructValue>> {
        use crate::types::ScalarRef;

        self.iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Hash)]
pub struct StructValue {
    fields: Box<[Datum]>,
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

impl StructValue {
    pub fn new(fields: Vec<Datum>) -> Self {
        Self {
            fields: fields.into_boxed_slice(),
        }
    }

    pub fn fields(&self) -> &[Datum] {
        &self.fields
    }

    pub fn memcmp_deserialize(
        fields: &[DataType],
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        fields
            .iter()
            .map(|field| memcmp_deserialize_datum_from(field, deserializer))
            .try_collect()
            .map(Self::new)
    }
}

#[derive(Copy, Clone)]
pub enum StructRef<'a> {
    Indexed { arr: &'a StructArray, idx: usize },
    ValueRef { val: &'a StructValue },
}

#[macro_export]
macro_rules! iter_fields_ref {
    ($self:ident, $it:ident, { $($body:tt)* }) => {
        match $self {
            StructRef::Indexed { arr, idx } => {
                let $it = arr.children.iter().map(|a| a.value_at(*idx));
                $($body)*
            }
            StructRef::ValueRef { val } => {
                let $it = val.fields.iter().map(ToDatumRef::to_datum_ref);
                $($body)*
            }
        }
    };
}

impl<'a> StructRef<'a> {
    pub fn fields_ref(&self) -> Vec<DatumRef<'a>> {
        iter_fields_ref!(self, it, { it.collect() })
    }

    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        iter_fields_ref!(self, it, {
            for datum_ref in it {
                memcmp_serialize_datum_into(datum_ref, serializer)?
            }
            Ok(())
        })
    }

    pub fn hash_scalar_inner<H: std::hash::Hasher>(&self, state: &mut H) {
        iter_fields_ref!(self, it, {
            for datum_ref in it {
                hash_datum(datum_ref, state);
            }
        })
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

fn cmp_struct_field(l: &Option<ScalarRefImpl<'_>>, r: &Option<ScalarRefImpl<'_>>) -> Ordering {
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
        let mut is_first = true;
        iter_fields_ref!(self, it, {
            for v in it {
                if is_first {
                    write!(f, "{:?}", v)?;
                    is_first = false;
                } else {
                    write!(f, ", {:?}", v)?;
                }
            }
            Ok(())
        })
    }
}

impl ToText for StructRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        iter_fields_ref!(self, it, {
            write!(f, "(")?;
            let mut is_first = true;
            for x in it {
                if is_first {
                    is_first = false;
                } else {
                    write!(f, ",")?;
                }
                ToText::write(&x, f)?;
            }
            write!(f, ")")
        })
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            DataType::Struct(_) => self.write(f),
            _ => unreachable!(),
        }
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
        let arr = StructArray::from_slices(&[true, false, true, false], vec![], vec![]);
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
        );
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
                children_names: Arc::new([]),
            },
        );
        for v in &struct_values {
            builder.append(v.as_ref().map(|s| s.as_scalar_ref()));
        }
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
        );
        let builder = arr.create_builder(4);
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
    fn test_serialize_deserialize() {
        let value = StructValue::new(vec![
            Some(OrderedF32::from(3.2).to_scalar_value()),
            Some("abcde".into()),
            Some(
                StructValue::new(vec![
                    Some(OrderedF64::from(1.3).to_scalar_value()),
                    Some("a".into()),
                    None,
                    Some(StructValue::new(vec![]).to_scalar_value()),
                ])
                .to_scalar_value(),
            ),
            None,
            Some("".into()),
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
        struct_ref.memcmp_serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            StructValue::memcmp_deserialize(&fields, &mut deserializer).unwrap(),
            value
        );

        let mut builder = StructArrayBuilder::with_meta(
            0,
            ArrayMeta::Struct {
                children: Arc::new(fields.clone()),
                children_names: Arc::new([]),
            },
        );
        builder.append(Some(struct_ref));
        let array = builder.finish();
        let struct_ref = array.value_at(0).unwrap();
        let mut serializer = memcomparable::Serializer::new(vec![]);
        struct_ref.memcmp_serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(
            StructValue::memcmp_deserialize(&fields, &mut deserializer).unwrap(),
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
                StructValue::new(vec![Some("".into())]),
                StructValue::new(vec![None]),
                vec![DataType::Varchar],
                Ordering::Less,
            ),
            (
                StructValue::new(vec![Some("abcd".into()), None]),
                StructValue::new(vec![
                    Some("abcd".into()),
                    Some(StructValue::new(vec![Some("abcdef".into())]).to_scalar_value()),
                ]),
                vec![
                    DataType::Varchar,
                    DataType::new_struct(vec![DataType::Varchar], vec![]),
                ],
                Ordering::Greater,
            ),
            (
                StructValue::new(vec![
                    Some("abcd".into()),
                    Some(StructValue::new(vec![Some("abcdef".into())]).to_scalar_value()),
                ]),
                StructValue::new(vec![
                    Some("abcd".into()),
                    Some(StructValue::new(vec![Some("abcdef".into())]).to_scalar_value()),
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
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                StructRef::ValueRef { val: &rhs }
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), order);

            let mut builder = StructArrayBuilder::with_meta(
                0,
                ArrayMeta::Struct {
                    children: Arc::from(fields),
                    children_names: Arc::new([]),
                },
            );
            builder.append(Some(StructRef::ValueRef { val: &lhs }));
            builder.append(Some(StructRef::ValueRef { val: &rhs }));
            let array = builder.finish();
            let lhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                array
                    .value_at(0)
                    .unwrap()
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs_serialized = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                array
                    .value_at(1)
                    .unwrap()
                    .memcmp_serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs_serialized.cmp(&rhs_serialized), order);
        }
    }
}
