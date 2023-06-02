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
use either::Either;
use itertools::Itertools;
use risingwave_pb::data::{PbArray, PbArrayType, StructArrayData};

use super::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayResult, DataChunk};
use crate::array::ArrayRef;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;
use crate::types::{
    hash_datum, DataType, Datum, DatumRef, DefaultPartialOrd, Scalar, StructType, ToDatumRef,
    ToText,
};
use crate::util::iter_util::ZipEqFast;
use crate::util::memcmp_encoding;
use crate::util::value_encoding::estimate_serialize_datum_size;

macro_rules! iter_fields_ref {
    ($self:expr, $it:ident, { $($body:tt)* }) => {
        iter_fields_ref!($self, $it, { $($body)* }, { $($body)* })
    };

    ($self:expr, $it:ident, { $($l_body:tt)* }, { $($r_body:tt)* }) => {
        match $self {
            StructRef::Indexed { arr, idx } => {
                let $it = arr.children.iter().map(move |a| a.value_at(idx));
                $($l_body)*
            }
            StructRef::ValueRef { val } => {
                let $it = val.fields.iter().map(ToDatumRef::to_datum_ref);
                $($r_body)*
            }
        }
    }
}

#[derive(Debug)]
pub struct StructArrayBuilder {
    bitmap: BitmapBuilder,
    pub(super) children_array: Vec<ArrayBuilderImpl>,
    type_: StructType,
    len: usize,
}

impl ArrayBuilder for StructArrayBuilder {
    type ArrayType = StructArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Self {
        panic!("Must use with_type.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_type(capacity, DataType::Struct(StructType::empty()))
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        let DataType::Struct(ty) = ty else {
            panic!("must be DataType::Struct");
        };
        let children_array = ty
            .types()
            .map(|a| a.create_array_builder(capacity))
            .collect();
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            children_array,
            type_: ty,
            len: 0,
        }
    }

    fn append_n(&mut self, n: usize, value: Option<StructRef<'_>>) {
        match value {
            None => {
                self.bitmap.append_n(n, false);
                for child in &mut self.children_array {
                    child.append_n(n, Datum::None);
                }
            }
            Some(v) => {
                self.bitmap.append_n(n, true);
                iter_fields_ref!(v, fields, {
                    for (child, f) in self.children_array.iter_mut().zip_eq_fast(fields) {
                        child.append_n(n, f);
                    }
                });
            }
        }
        self.len += n;
    }

    fn append_array(&mut self, other: &StructArray) {
        self.bitmap.append_bitmap(&other.bitmap);
        for (a, o) in self.children_array.iter_mut().zip_eq_fast(&other.children) {
            a.append_array(o);
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

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> StructArray {
        let children = self
            .children_array
            .into_iter()
            .map(|b| Arc::new(b.finish()))
            .collect::<Vec<ArrayRef>>();
        StructArray::new(self.type_, children, self.bitmap.finish())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructArray {
    bitmap: Bitmap,
    children: Vec<ArrayRef>,
    type_: StructType,
    heap_size: usize,
}

impl StructArrayBuilder {
    pub fn append_array_refs(&mut self, refs: Vec<ArrayRef>, len: usize) {
        self.bitmap.append_n(len, true);
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
        self.bitmap.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let children_array = self.children.iter().map(|a| a.to_protobuf()).collect();
        let children_type = self.type_.types().map(|t| t.to_protobuf()).collect();
        PbArray {
            array_type: PbArrayType::Struct as i32,
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

    fn data_type(&self) -> DataType {
        DataType::Struct(self.type_.clone())
    }
}

impl StructArray {
    pub fn new(type_: StructType, children: Vec<ArrayRef>, bitmap: Bitmap) -> Self {
        let heap_size = bitmap.estimated_heap_size()
            + children
                .iter()
                .map(|c| c.estimated_heap_size())
                .sum::<usize>();

        Self {
            bitmap,
            children,
            type_,
            heap_size,
        }
    }

    pub fn from_protobuf(array: &PbArray) -> ArrayResult<ArrayImpl> {
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
        let type_ = StructType::unnamed(
            array_data
                .children_type
                .iter()
                .map(DataType::from)
                .collect(),
        );
        Ok(Self::new(type_, children, bitmap).into())
    }

    /// Returns an iterator over the field array.
    pub fn fields(&self) -> impl ExactSizeIterator<Item = &ArrayRef> {
        self.children.iter()
    }

    pub fn field_at(&self, index: usize) -> &ArrayRef {
        &self.children[index]
    }

    #[cfg(test)]
    pub fn values_vec(&self) -> Vec<Option<StructValue>> {
        use crate::types::ScalarRef;

        self.iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec()
    }
}

impl EstimateSize for StructArray {
    fn estimated_heap_size(&self) -> usize {
        self.heap_size
    }
}

impl From<DataChunk> for StructArray {
    fn from(chunk: DataChunk) -> Self {
        Self::new(
            StructType::unnamed(chunk.columns().iter().map(|c| c.data_type()).collect()),
            chunk.columns().to_vec(),
            chunk.vis().to_bitmap(),
        )
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

impl EstimateSize for StructValue {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Try speed up this process.
        self.fields
            .iter()
            .map(|datum| datum.estimated_heap_size())
            .sum()
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

    pub fn memcmp_deserialize<'a>(
        fields: impl IntoIterator<Item = &'a DataType>,
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        fields
            .into_iter()
            .map(|field| memcmp_encoding::deserialize_datum_in_composite(field, deserializer))
            .try_collect()
            .map(Self::new)
    }
}

#[derive(Copy, Clone)]
pub enum StructRef<'a> {
    Indexed { arr: &'a StructArray, idx: usize },
    ValueRef { val: &'a StructValue },
}

impl<'a> StructRef<'a> {
    /// Iterates over the fields of the struct.
    ///
    /// Prefer using the macro `iter_fields_ref!` if possible to avoid the cost of enum dispatching.
    pub fn iter_fields_ref(self) -> impl ExactSizeIterator<Item = DatumRef<'a>> + 'a {
        iter_fields_ref!(self, it, { Either::Left(it) }, { Either::Right(it) })
    }

    pub fn memcmp_serialize(
        self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        iter_fields_ref!(self, it, {
            for datum_ref in it {
                memcmp_encoding::serialize_datum_in_composite(datum_ref, serializer)?
            }
            Ok(())
        })
    }

    pub fn hash_scalar_inner<H: std::hash::Hasher>(self, state: &mut H) {
        iter_fields_ref!(self, it, {
            for datum_ref in it {
                hash_datum(datum_ref, state);
            }
        })
    }

    pub fn estimate_serialize_size_inner(self) -> usize {
        iter_fields_ref!(self, it, {
            it.fold(0, |acc, datum_ref| {
                acc + estimate_serialize_datum_size(datum_ref)
            })
        })
    }
}

impl PartialEq for StructRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        iter_fields_ref!(*self, lhs, {
            iter_fields_ref!(*other, rhs, { lhs.eq(rhs) })
        })
    }
}

impl PartialOrd for StructRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        iter_fields_ref!(*self, lhs, {
            iter_fields_ref!(*other, rhs, {
                if lhs.len() != rhs.len() {
                    return None;
                }
                lhs.partial_cmp_by(rhs, |lv, rv| lv.default_partial_cmp(&rv))
            })
        })
    }
}

impl Debug for StructRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut is_first = true;
        iter_fields_ref!(*self, it, {
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
        iter_fields_ref!(*self, it, {
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
    use crate::try_match_expand;
    use crate::types::{F32, F64};

    // Empty struct is allowed in postgres.
    // `CREATE TYPE foo_empty as ();`, e.g.
    #[test]
    fn test_struct_new_empty() {
        let arr = StructArray::new(StructType::empty(), vec![], Bitmap::ones(0));
        let actual = StructArray::from_protobuf(&arr.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);
    }

    #[test]
    fn test_struct_with_fields() {
        use crate::array::*;
        let arr = StructArray::new(
            StructType::unnamed(vec![DataType::Int32, DataType::Float32]),
            vec![
                I32Array::from_iter([None, Some(1), None, Some(2)]).into_ref(),
                F32Array::from_iter([None, Some(3.0), None, Some(4.0)]).into_ref(),
            ],
            [false, true, false, true].into_iter().collect(),
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

        let mut builder = StructArrayBuilder::with_type(
            4,
            DataType::Struct(StructType::unnamed(vec![
                DataType::Int32,
                DataType::Float32,
            ])),
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
        let arr = StructArray::new(
            StructType::unnamed(vec![DataType::Int32, DataType::Float32]),
            vec![
                I32Array::from_iter([Some(1)]).into_ref(),
                F32Array::from_iter([Some(2.0)]).into_ref(),
            ],
            Bitmap::ones(1),
        );
        let builder = arr.create_builder(4);
        let arr2 = builder.finish();
        assert_eq!(arr.data_type(), arr2.data_type());
    }

    #[test]
    fn test_struct_value_cmp() {
        // (1, 2.0) > (1, 1.0)
        assert_gt!(
            StructValue::new(vec![Some(1.into()), Some(2.0.into())]),
            StructValue::new(vec![Some(1.into()), Some(1.0.into())]),
        );
        // null > 1
        assert_gt!(
            StructValue::new(vec![None]),
            StructValue::new(vec![Some(1.into())]),
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
            Some(F32::from(3.2).to_scalar_value()),
            Some("abcde".into()),
            Some(
                StructValue::new(vec![
                    Some(F64::from(1.3).to_scalar_value()),
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

        let mut builder = StructArrayBuilder::with_type(
            0,
            DataType::Struct(StructType::unnamed(fields.to_vec())),
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

            let mut builder = StructArrayBuilder::with_type(
                0,
                DataType::Struct(StructType::unnamed(fields.to_vec())),
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
