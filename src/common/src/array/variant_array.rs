// Copyright 2026 RisingWave Labs
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

use std::mem::{size_of, size_of_val};

use bytes::Bytes;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::common::Buffer;
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::data::{PbArray, PbArrayType};

use super::{Array, ArrayBuilder, ArrayImpl, ArrayResult};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, Scalar, VariantRef, VariantVal};

#[derive(Debug, Clone, EstimateSize)]
pub struct VariantArrayBuilder {
    bitmap: BitmapBuilder,
    offsets: Vec<u32>,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
pub struct VariantArray {
    bitmap: Bitmap,
    offsets: Vec<u32>,
    data: Bytes,
}

impl ArrayBuilder for VariantArrayBuilder {
    type ArrayType = VariantArray;

    fn new(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            offsets,
            data: Vec::new(),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, DataType::Variant);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        match value {
            Some(value) => {
                let bytes = value.value_serialize();
                for _ in 0..n {
                    self.bitmap.append(true);
                    self.data.extend_from_slice(&bytes);
                    self.push_current_offset();
                }
            }
            None => {
                for _ in 0..n {
                    self.bitmap.append(false);
                    self.push_current_offset();
                }
            }
        }
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        for idx in 0..other.len() {
            self.append(other.value_at(idx));
        }
    }

    fn pop(&mut self) -> Option<()> {
        self.bitmap.pop()?;
        self.offsets.pop();
        let last_offset = *self.offsets.last().expect("offsets always has one element") as usize;
        self.data.truncate(last_offset);
        Some(())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> Self::ArrayType {
        Self::ArrayType {
            bitmap: self.bitmap.finish(),
            offsets: self.offsets,
            data: Bytes::from(self.data),
        }
    }
}

impl VariantArrayBuilder {
    fn push_current_offset(&mut self) {
        let offset = u32::try_from(self.data.len()).expect("variant array data exceeds u32::MAX");
        self.offsets.push(offset);
    }
}

impl VariantArray {
    pub fn from_protobuf(array: &PbArray) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.values.len() == 2,
            "Must have exactly 2 buffers in a variant array"
        );

        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        let offsets = decode_offsets(&array.values[0].body)?;
        ensure!(
            offsets.len() == bitmap.len() + 1,
            "variant offsets length must equal array length + 1"
        );
        let data = Bytes::copy_from_slice(&array.values[1].body);
        validate_offsets(&offsets, data.len())?;
        validate_serialized_values(&bitmap, &offsets, &data)?;

        Ok(VariantArray {
            bitmap,
            offsets,
            data,
        }
        .into())
    }

    fn serialized_at_unchecked(&self, idx: usize) -> &[u8] {
        unsafe {
            let begin = *self.offsets.get_unchecked(idx) as usize;
            let end = *self.offsets.get_unchecked(idx + 1) as usize;
            self.data.get_unchecked(begin..end)
        }
    }
}

impl Array for VariantArray {
    type Builder = VariantArrayBuilder;
    type OwnedItem = VariantVal;
    type RefItem<'a> = VariantRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        VariantRef::from_serialized(self.serialized_at_unchecked(idx))
            .expect("variant array element should contain valid variant bytes")
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn to_protobuf(&self) -> PbArray {
        PbArray {
            null_bitmap: Some(self.null_bitmap().to_protobuf()),
            values: vec![
                Buffer {
                    compression: CompressionType::None as i32,
                    body: encode_offsets(&self.offsets),
                },
                Buffer {
                    compression: CompressionType::None as i32,
                    body: self.data.to_vec(),
                },
            ],
            array_type: PbArrayType::Variant as i32,
            struct_array_data: None,
            list_array_data: None,
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        assert_eq!(bitmap.len(), self.len());
        self.bitmap = bitmap;
    }

    fn data_type(&self) -> DataType {
        DataType::Variant
    }
}

impl FromIterator<Option<VariantVal>> for VariantArray {
    fn from_iter<I: IntoIterator<Item = Option<VariantVal>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            match i {
                Some(x) => builder.append(Some(x.as_scalar_ref())),
                None => builder.append(None),
            }
        }
        builder.finish()
    }
}

impl FromIterator<VariantVal> for VariantArray {
    fn from_iter<I: IntoIterator<Item = VariantVal>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

fn encode_offsets(offsets: &[u32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size_of_val(offsets));
    for offset in offsets {
        buf.extend_from_slice(&offset.to_le_bytes());
    }
    buf
}

fn decode_offsets(buf: &[u8]) -> ArrayResult<Vec<u32>> {
    ensure!(
        buf.len().is_multiple_of(size_of::<u32>()),
        "variant offset buffer length must be a multiple of 4"
    );
    Ok(buf
        .chunks_exact(size_of::<u32>())
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

fn validate_offsets(offsets: &[u32], data_len: usize) -> ArrayResult<()> {
    ensure!(
        offsets.first() == Some(&0),
        "variant offsets must start from 0"
    );
    ensure!(
        offsets.last().copied().unwrap_or_default() as usize == data_len,
        "variant final offset must equal data length"
    );
    ensure!(
        offsets.windows(2).all(|pair| pair[0] <= pair[1]),
        "variant offsets must be monotonic"
    );
    Ok(())
}

fn validate_serialized_values(bitmap: &Bitmap, offsets: &[u32], data: &[u8]) -> ArrayResult<()> {
    for (idx, not_null) in bitmap.iter().enumerate() {
        if !not_null {
            continue;
        }
        let begin = offsets[idx] as usize;
        let end = offsets[idx + 1] as usize;
        ensure!(
            VariantRef::from_serialized(&data[begin..end]).is_some(),
            "variant array element {idx} must use current valid variant encoding"
        );
    }
    Ok(())
}
