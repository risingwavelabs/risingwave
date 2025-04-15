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
    fn new(_capacity: usize) -> Self {
        todo!("VECTOR_PLACEHOLDER")
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
        self.inner.append_n(n, value.map(|v| v.inner))
    }

    fn append_array(&mut self, other: &VectorArray) {
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
        todo!("VECTOR_PLACEHOLDER")
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

#[derive(Clone, EstimateSize)]
pub struct VectorVal {
    inner: ListValue,
}

impl Debug for VectorVal {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("VECTOR_PLACEHOLDER")
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

#[derive(Clone, Copy)]
pub struct VectorRef<'a> {
    inner: ListRef<'a>,
}

impl Debug for VectorRef<'_> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("VECTOR_PLACEHOLDER")
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
    fn write<W: std::fmt::Write>(&self, _f: &mut W) -> std::fmt::Result {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn write_with_type<W: std::fmt::Write>(&self, _ty: &DataType, _f: &mut W) -> std::fmt::Result {
        todo!("VECTOR_PLACEHOLDER")
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
