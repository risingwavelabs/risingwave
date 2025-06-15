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

use super::{Array, ArrayBuilder};
use crate::bitmap::Bitmap;
use crate::types::{DataType, Scalar, ScalarRef, ToText};

#[derive(Debug, Clone, EstimateSize)]
pub struct VectorArrayBuilder {}

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

    fn with_type(_capacity: usize, _ty: DataType) -> Self {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn append_n(&mut self, _n: usize, _value: Option<VectorRef<'_>>) {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn append_array(&mut self, _other: &VectorArray) {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn pop(&mut self) -> Option<()> {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn len(&self) -> usize {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn finish(self) -> VectorArray {
        todo!("VECTOR_PLACEHOLDER")
    }
}

#[derive(Debug, Clone)]
pub struct VectorArray {}

impl EstimateSize for VectorArray {
    fn estimated_heap_size(&self) -> usize {
        todo!("VECTOR_PLACEHOLDER")
    }
}

impl Array for VectorArray {
    type Builder = VectorArrayBuilder;
    type OwnedItem = VectorVal;
    type RefItem<'a> = VectorRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, _idx: usize) -> Self::RefItem<'_> {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn len(&self) -> usize {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn to_protobuf(&self) -> PbArray {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn null_bitmap(&self) -> &Bitmap {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn into_null_bitmap(self) -> Bitmap {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn set_bitmap(&mut self, _bitmap: Bitmap) {
        todo!("VECTOR_PLACEHOLDER")
    }

    fn data_type(&self) -> DataType {
        todo!("VECTOR_PLACEHOLDER")
    }
}

#[derive(Clone, EstimateSize)]
pub struct VectorVal {}

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
        todo!("VECTOR_PLACEHOLDER")
    }
}

#[derive(Clone, Copy)]
pub struct VectorRef<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
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
        todo!("VECTOR_PLACEHOLDER")
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, _state: &mut H) {
        todo!("VECTOR_PLACEHOLDER")
    }
}
