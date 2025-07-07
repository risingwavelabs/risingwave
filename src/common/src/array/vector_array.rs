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

use std::fmt::Debug;

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::PbArray;

use super::{Array, ArrayBuilder, ListArray, ListArrayBuilder, ListRef, ListValue};
use crate::bitmap::Bitmap;
use crate::types::{DataType, Scalar, ScalarRef, ToText};

#[derive(Debug, Clone, EstimateSize)]
pub struct VectorArrayBuilder {
    inner: ListArrayBuilder,
    elem_size: usize,
}

impl ArrayBuilder for VectorArrayBuilder {
    type ArrayType = VectorArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Self {
        panic!("please use `VectorArrayBuilder::with_type` instead");
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_type(capacity, DataType::Vector(3))
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        let DataType::Vector(elem_size) = ty else {
            panic!("VectorArrayBuilder only supports Vector type");
        };
        Self {
            inner: ListArrayBuilder::with_type(capacity, DataType::List(DataType::Float32.into())),
            elem_size,
        }
    }

    fn append_n(&mut self, n: usize, value: Option<VectorRef<'_>>) {
        if let Some(value) = value {
            assert_eq!(self.elem_size, value.inner.len());
        }
        self.inner.append_n(n, value.map(|v| v.inner))
    }

    fn append_array(&mut self, other: &VectorArray) {
        assert_eq!(self.elem_size, other.elem_size);
        self.inner.append_array(&other.inner)
    }

    fn pop(&mut self) -> Option<()> {
        self.inner.pop()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn finish(self) -> VectorArray {
        VectorArray {
            inner: self.inner.finish(),
            elem_size: self.elem_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VectorArray {
    inner: ListArray,
    elem_size: usize,
}

impl EstimateSize for VectorArray {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}

impl Array for VectorArray {
    type Builder = VectorArrayBuilder;
    type OwnedItem = VectorVal;
    type RefItem<'a> = VectorRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        VectorRef {
            inner: unsafe { self.inner.raw_value_at_unchecked(idx) },
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let mut pb_array = self.inner.to_protobuf();
        pb_array.set_array_type(risingwave_pb::data::PbArrayType::Vector);
        pb_array.list_array_data.as_mut().unwrap().elem_size = Some(self.elem_size as _);
        pb_array
    }

    fn null_bitmap(&self) -> &Bitmap {
        self.inner.null_bitmap()
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.inner.into_null_bitmap()
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.inner.set_bitmap(bitmap)
    }

    fn data_type(&self) -> DataType {
        DataType::Vector(self.elem_size)
    }
}

impl VectorArray {
    pub fn from_protobuf(
        pb_array: &risingwave_pb::data::PbArray,
    ) -> super::ArrayResult<super::ArrayImpl> {
        let inner = ListArray::from_protobuf(pb_array)?.into_list();
        let elem_size = pb_array
            .list_array_data
            .as_ref()
            .unwrap()
            .elem_size
            .unwrap() as _;
        Ok(Self { inner, elem_size }.into())
    }
}

#[derive(Clone, EstimateSize)]
pub struct VectorVal {
    inner: ListValue,
}

impl Debug for VectorVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_scalar_ref().fmt(f)
    }
}

impl PartialEq for VectorVal {
    fn eq(&self, _other: &Self) -> bool {
        todo!("VECTOR_PLACEHOLDER")
    }
}
impl Eq for VectorVal {}
impl PartialOrd for VectorVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for VectorVal {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        todo!("VECTOR_PLACEHOLDER")
    }
}

impl Scalar for VectorVal {
    type ScalarRefType<'a> = VectorRef<'a>;

    fn as_scalar_ref(&self) -> VectorRef<'_> {
        VectorRef {
            inner: self.inner.as_scalar_ref(),
        }
    }
}

impl VectorVal {
    pub fn from_text(text: &str, size: usize) -> Result<Self, String> {
        let text = text.trim();
        let text = text
            .strip_prefix('[')
            .ok_or_else(|| "vector must start with [".to_owned())?
            .strip_suffix(']')
            .ok_or_else(|| "vector must end with ]".to_owned())?;
        let inner = text
            .split(',')
            .map(|s| {
                s.trim()
                    .parse::<f32>()
                    .map_err(|_| format!("invalid f32: {s}"))
                    .and_then(|f| {
                        if f.is_finite() {
                            Ok(crate::types::F32::from(f))
                        } else {
                            Err(format!("{f} not allowed in vector"))
                        }
                    })
            })
            .collect::<Result<ListValue, _>>()?;
        if inner.len() != size {
            return Err(format!("expected {} dimensions, not {}", size, inner.len()));
        }
        Ok(Self { inner })
    }

    /// Create a new vector from inner [`ListValue`].
    ///
    /// This is leak of implementation. Prefer [`VectorVal::from_iter`] below.
    pub fn from_inner(inner: ListValue) -> Self {
        Self { inner }
    }
}

// The `F32` wrapping is unnecessary given nan/inf/-inf are not allowed in vector.
// There is not going to be `F16` for `halfvec` later; just `f16`.
// We keep it for now because the inner `List` type contains `PrimitiveArray<F32>`.
impl FromIterator<crate::types::F32> for VectorVal {
    fn from_iter<I: IntoIterator<Item = crate::types::F32>>(iter: I) -> Self {
        let inner = ListValue::from_iter(iter);
        Self { inner }
    }
}

impl FromIterator<f32> for VectorVal {
    fn from_iter<I: IntoIterator<Item = f32>>(iter: I) -> Self {
        let inner = ListValue::from_iter(iter.into_iter().map(crate::types::F32::from));
        Self { inner }
    }
}

#[derive(Clone, Copy)]
pub struct VectorRef<'a> {
    inner: ListRef<'a>,
}

impl Debug for VectorRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_with_type(&DataType::Vector(self.into_slice().len()), f)
    }
}

impl PartialEq for VectorRef<'_> {
    fn eq(&self, _other: &Self) -> bool {
        todo!("VECTOR_PLACEHOLDER")
    }
}
impl Eq for VectorRef<'_> {}
impl PartialOrd for VectorRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for VectorRef<'_> {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        todo!("VECTOR_PLACEHOLDER")
    }
}

impl ToText for VectorRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        self.write_with_type(&DataType::Vector(self.into_slice().len()), f)
    }

    fn write_with_type<W: std::fmt::Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, item) in self.inner.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            item.write_with_type(&DataType::Float32, f)?;
        }
        write!(f, "]")
    }
}

impl<'a> ScalarRef<'a> for VectorRef<'a> {
    type ScalarType = VectorVal;

    fn to_owned_scalar(&self) -> VectorVal {
        VectorVal {
            inner: self.inner.to_owned_scalar(),
        }
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash_scalar(state)
    }
}

impl<'a> VectorRef<'a> {
    /// Get the inner [`ListRef`].
    ///
    /// This is leak of implementation. Prefer [`Self::into_slice`] below.
    pub fn into_inner(self) -> ListRef<'a> {
        self.inner
    }

    /// Get the slice of floats in this vector.
    pub fn into_slice(self) -> &'a [f32] {
        crate::types::F32::inner_slice(self.inner.as_primitive_slice().unwrap())
    }
}
