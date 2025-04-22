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

use super::{Array, ArrayBuilder, ListRef, ListValue};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, Scalar, ScalarRef, ToText};

#[derive(Debug, Clone, EstimateSize)]
pub struct VectorArrayBuilder {
    bitmap: BitmapBuilder,
    values: Vec<f32>,
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
            bitmap: BitmapBuilder::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            elem_size,
        }
    }

    fn append_n(&mut self, n: usize, value: Option<VectorRef<'_>>) {
        match value {
            None => {
                self.bitmap.append_n(n, false);
                self.values
                    .extend(std::iter::repeat_n(f32::NAN, n * self.elem_size));
            }
            Some(v) => {
                assert_eq!(self.elem_size, v.as_slice().len());
                self.bitmap.append_n(n, true);
                for _ in 0..n {
                    self.values.extend(v.as_slice());
                }
            }
        }
    }

    fn append_array(&mut self, other: &VectorArray) {
        assert_eq!(self.elem_size, other.elem_size);
        self.bitmap.append_bitmap(&other.bitmap);
        self.values.extend_from_slice(&other.values);
    }

    fn pop(&mut self) -> Option<()> {
        self.bitmap.pop()?;
        self.values.truncate(self.values.len() - self.elem_size);
        Some(())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> VectorArray {
        VectorArray {
            bitmap: self.bitmap.finish(),
            values: self.values.into(),
            elem_size: self.elem_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VectorArray {
    bitmap: Bitmap,
    values: Box<[f32]>,
    elem_size: usize,
}

impl EstimateSize for VectorArray {
    fn estimated_heap_size(&self) -> usize {
        todo!("VECTOR_PLACEHOLDER")
    }
}

impl Array for VectorArray {
    type Builder = VectorArrayBuilder;
    type OwnedItem = VectorVal;
    type RefItem<'a> = VectorRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        VectorRef {
            values: unsafe {
                self.values
                    .get_unchecked((idx * self.elem_size)..((idx + 1) * self.elem_size))
            },
        }
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn to_protobuf(&self) -> PbArray {
        todo!("VECTOR_PLACEHOLDER")
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
        DataType::Vector(self.elem_size)
    }
}

impl VectorArray {
    pub fn from_protobuf(
        _pb_array: &risingwave_pb::data::PbArray,
    ) -> super::ArrayResult<super::ArrayImpl> {
        todo!("VECTOR_PLACEHOLDER")
    }
}

#[derive(Clone, EstimateSize)]
pub struct VectorVal {
    values: Box<[f32]>,
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
            values: &self.values,
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
                            Ok(f)
                        } else {
                            Err(format!("{f} not allowed in vector"))
                        }
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if inner.len() != size {
            return Err(format!("expected {} dimensions, not {}", size, inner.len()));
        }
        Ok(Self {
            values: inner.into(),
        })
    }

    pub fn from_inner(_inner: ListValue) -> Self {
        todo!("VECTOR_PLACEHOLDER")
    }
}

#[derive(Clone, Copy)]
pub struct VectorRef<'a> {
    values: &'a [f32],
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
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        // TODO: use the correct type; `Vector(0)` is invalid
        self.write_with_type(&DataType::Vector(0), f)
    }

    fn write_with_type<W: std::fmt::Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, item) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, "]")
    }
}

impl<'a> ScalarRef<'a> for VectorRef<'a> {
    type ScalarType = VectorVal;

    fn to_owned_scalar(&self) -> VectorVal {
        VectorVal {
            values: self.values.into(),
        }
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, _state: &mut H) {
        todo!("VECTOR_PLACEHOLDER")
    }
}

impl<'a> VectorRef<'a> {
    pub fn as_slice(self) -> &'a [f32] {
        self.values
    }

    pub fn into_inner(self) -> ListRef<'a> {
        todo!("VECTOR_PLACEHOLDER")
    }
}
