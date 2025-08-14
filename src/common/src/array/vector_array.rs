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
use std::hash::Hash;
use std::slice;

use bytes::{Buf, BufMut};
use itertools::{Itertools, repeat_n};
use memcomparable::Error;
use risingwave_common::types::F32;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::common::PbBuffer;
use risingwave_pb::common::buffer::PbCompressionType;
use risingwave_pb::data::{PbArray, PbArrayType, PbListArrayData};
use serde::{Deserialize, Serialize};

use super::{Array, ArrayBuilder};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, Scalar, ScalarRef, ToText};
use crate::vector::{VectorInner, decode_vector_payload, encode_vector_payload};

pub type VectorItemType = F32;
pub type VectorDistanceType = f64;
pub const VECTOR_ITEM_TYPE: DataType = DataType::Float32;
pub const VECTOR_DISTANCE_TYPE: DataType = DataType::Float64;

#[derive(Debug, Clone, EstimateSize)]
pub struct VectorArrayBuilder {
    bitmap: BitmapBuilder,
    offsets: Vec<u32>,
    inner: Vec<VectorItemType>,
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
        Self::with_type(capacity, VectorVal::test_type())
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        let DataType::Vector(elem_size) = ty else {
            panic!("VectorArrayBuilder only supports Vector type");
        };
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            offsets,
            inner: Vec::with_capacity(capacity * elem_size),
            elem_size,
        }
    }

    fn append_n(&mut self, n: usize, value: Option<VectorRef<'_>>) {
        let last = self
            .offsets
            .last()
            .cloned()
            .expect("non-empty with an initial 0");
        if let Some(value) = value {
            assert_eq!(self.elem_size, value.inner.len());
            self.inner.reserve(self.elem_size * n);
            for _ in 0..n {
                self.inner.extend_from_slice(value.inner);
            }
            self.offsets.reserve(n);
            self.offsets.extend((1..=n).map(|i| {
                last.checked_add((i * self.elem_size) as _)
                    .expect("overflow")
            }));
            self.bitmap.append_n(n, true);
        } else {
            self.offsets.reserve(n);
            self.offsets.extend(repeat_n(last, n));
            self.bitmap.append_n(n, false);
        }
    }

    fn append_array(&mut self, other: &VectorArray) {
        assert_eq!(self.elem_size, other.elem_size);
        self.bitmap.append_bitmap(&other.bitmap);
        let last = self
            .offsets
            .last()
            .cloned()
            .expect("non-empty with an initial 0");
        let other_offsets = &other.offsets[1..];
        self.offsets.reserve(other_offsets.len());
        self.offsets.extend(
            other_offsets
                .iter()
                .map(|offset| last.checked_add(*offset).expect("overflow")),
        );
        self.inner.reserve(other.inner.len());
        self.inner.extend_from_slice(&other.inner);
    }

    fn pop(&mut self) -> Option<()> {
        if self.bitmap.pop().is_some() {
            self.offsets
                .pop()
                .expect("non-empty when bitmap popped Some");
            let last = self
                .offsets
                .last()
                .cloned()
                .expect("non-empty with initial 0");
            self.inner.truncate(last as _);
            Some(())
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> VectorArray {
        VectorArray {
            bitmap: self.bitmap.finish(),
            offsets: self.offsets,
            inner: self.inner,
            elem_size: self.elem_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VectorArray {
    bitmap: Bitmap,
    /// Of size as `bitmap.len() + 1`. `(self.offsets[i]..self.offsets[i+1])` is the slice range of the i-th vector
    /// if it's not null.
    offsets: Vec<u32>,
    inner: Vec<VectorItemType>,
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
            inner: unsafe {
                let start = self.inner.as_ptr().add(self.offsets[idx] as usize);
                slice::from_raw_parts(start, self.elem_size)
            },
        }
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let mut payload = Vec::with_capacity(self.inner.len() * size_of::<VectorItemType>());
        encode_vector_payload(self.inner.as_slice(), &mut payload);
        PbArray {
            array_type: PbArrayType::Vector as _,
            null_bitmap: Some(self.bitmap.to_protobuf()),
            values: vec![PbBuffer {
                compression: PbCompressionType::None as _,
                body: payload,
            }],
            struct_array_data: None,
            list_array_data: Some(
                PbListArrayData {
                    offsets: self.offsets.clone(),
                    value: None,
                    value_type: Some(DataType::Float32.to_protobuf()),
                    elem_size: Some(self.elem_size as _),
                }
                .into(),
            ),
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
        DataType::Vector(self.elem_size)
    }
}

impl VectorArray {
    pub fn from_protobuf(
        array: &risingwave_pb::data::PbArray,
    ) -> super::ArrayResult<super::ArrayImpl> {
        // reversing to_protobuf
        assert_eq!(
            array.array_type,
            PbArrayType::Vector as i32,
            "invalid array type for vector: {}",
            array.array_type
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        let encoded_payload = &array.values[0].body;
        let payload = decode_vector_payload(
            encoded_payload
                .len()
                .checked_exact_div(size_of::<VectorItemType>())
                .unwrap_or_else(|| {
                    panic!("invalid payload len {} for vector", encoded_payload.len(),)
                }),
            array.values[0].body.as_slice(),
        );
        let array_data = array.get_list_array_data()?;
        let elem_size = array_data.elem_size.expect("should exist for Vector") as usize;
        let offsets = array_data.offsets.clone();
        debug_assert_eq!(array_data.value_type, Some(DataType::Float32.to_protobuf()));
        debug_assert_eq!(array_data.value, None);

        Ok(VectorArray {
            bitmap,
            offsets,
            inner: payload,
            elem_size,
        }
        .into())
    }

    pub fn as_raw_slice(&self) -> &[f32] {
        F32::inner_slice(&self.inner)
    }

    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }
}

pub type VectorVal = VectorInner<Box<[VectorItemType]>>;

impl Debug for VectorVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_scalar_ref().fmt(f)
    }
}

impl Scalar for VectorVal {
    type ScalarRefType<'a> = VectorRef<'a>;

    fn as_scalar_ref(&self) -> VectorRef<'_> {
        VectorInner { inner: &self.inner }
    }
}

impl VectorVal {
    #[cfg(test)]
    pub const TEST_VECTOR_DIMENSION: usize = 3;

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
                            Ok(f.into())
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
            inner: inner.into(),
        })
    }

    #[cfg(test)]
    pub fn test_type() -> DataType {
        DataType::Vector(Self::TEST_VECTOR_DIMENSION)
    }

    pub fn to_ref(&self) -> VectorRef<'_> {
        VectorRef { inner: &self.inner }
    }
}

/// A `f32` without nan/inf/-inf. Added as intermediate type to `try_collect` `f32` values into a `VectorVal`.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Finite32(f32);

impl TryFrom<f32> for Finite32 {
    type Error = String;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(format!("{value} not allowed in vector"))
        }
    }
}

impl From<Vec<Finite32>> for VectorVal {
    fn from(value: Vec<Finite32>) -> Self {
        let (ptr, len, cap) = value.into_raw_parts();
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values.
        Self {
            inner: unsafe { Vec::from_raw_parts(ptr as *mut F32, len, cap).into_boxed_slice() },
        }
    }
}

impl FromIterator<Finite32> for VectorVal {
    fn from_iter<I: IntoIterator<Item = Finite32>>(iter: I) -> Self {
        let inner = iter.into_iter().collect_vec();
        Self::from(inner)
    }
}

pub type VectorRef<'a> = VectorInner<&'a [VectorItemType]>;

impl Debug for VectorRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write_with_type(&DataType::Vector(self.dimension()), f)
    }
}

impl ToText for VectorRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        self.write_with_type(&DataType::Vector(self.dimension()), f)
    }

    fn write_with_type<W: std::fmt::Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, item) in self.inner.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", item)?;
        }
        write!(f, "]")
    }
}

impl<'a> ScalarRef<'a> for VectorRef<'a> {
    type ScalarType = VectorVal;

    fn to_owned_scalar(&self) -> VectorVal {
        VectorVal {
            inner: self.inner.to_vec().into_boxed_slice(),
        }
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<'a> VectorRef<'a> {
    pub fn from_slice(inner: &'a [VectorItemType]) -> Self {
        Self { inner }
    }

    pub fn memcmp_serialize(
        self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        for item in self.inner {
            item.serialize(&mut *serializer)?;
        }
        Ok(())
    }
}

impl VectorVal {
    pub fn memcmp_deserialize(
        dimension: usize,
        de: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let mut value = Vec::with_capacity(dimension);
        for _ in 0..dimension {
            value.push(Finite32::try_from(f32::deserialize(&mut *de)?).map_err(Error::Message)?)
        }
        Ok(VectorVal::from(value))
    }
}
