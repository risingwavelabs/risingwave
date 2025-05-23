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

use std::io::{Cursor, Read};

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::common::Buffer;
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::data::PbArray;

use crate::array::{Array, ArrayBuilder, ArrayImpl, ArrayResult};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, Scalar, Uuid, UuidRef}; // Added ScalarRef import here

/// Builder for `UuidArray`.
#[derive(Debug, Clone)]
pub struct UuidArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<uuid::Uuid>,
}

/// Array of UUIDs. Each element is a 16-byte value that represents a UUID.
#[derive(Debug, Clone, PartialEq)]
pub struct UuidArray {
    bitmap: Bitmap,
    data: Box<[uuid::Uuid]>,
}

// Implement EstimateSize manually for UuidArray
impl EstimateSize for UuidArray {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size() + self.data.len() * std::mem::size_of::<uuid::Uuid>()
    }
}

// Implement EstimateSize manually for UuidArrayBuilder
impl EstimateSize for UuidArrayBuilder {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size() + self.data.capacity() * std::mem::size_of::<uuid::Uuid>()
    }
}

impl Array for UuidArray {
    type Builder = UuidArrayBuilder;
    type OwnedItem = Uuid;
    type RefItem<'a> = UuidRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        unsafe { UuidRef(self.data.get_unchecked(idx)) }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let mut output_buffer = Vec::<u8>::with_capacity(self.len() * Uuid::size());

        for v in self.iter() {
            if let Some(uuid_ref) = v {
                let bytes = uuid_ref.to_be_bytes();
                output_buffer.extend_from_slice(&bytes);
            }
        }

        let buffer = Buffer {
            compression: CompressionType::None as i32,
            body: output_buffer,
        };

        PbArray {
            null_bitmap: Some(self.null_bitmap().to_protobuf()),
            values: vec![buffer],
            array_type: Uuid::array_type() as i32,
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
        self.bitmap = bitmap;
    }

    fn data_type(&self) -> DataType {
        DataType::Uuid
    }
}

impl ArrayBuilder for UuidArrayBuilder {
    type ArrayType = UuidArray;

    fn new(capacity: usize) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, DataType::Uuid);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data.extend(std::iter::repeat(*x.0).take(n));
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data
                    .extend(std::iter::repeat(uuid::Uuid::nil()).take(n));
            }
        }
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> Self::ArrayType {
        Self::ArrayType {
            bitmap: self.bitmap.finish(),
            data: self.data.into(),
        }
    }
}

impl<'a> FromIterator<Option<UuidRef<'a>>> for UuidArray {
    fn from_iter<T: IntoIterator<Item = Option<UuidRef<'a>>>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
    }
}

impl FromIterator<Uuid> for UuidArray {
    fn from_iter<I: IntoIterator<Item = Uuid>>(iter: I) -> Self {
        let data: Box<[uuid::Uuid]> = iter.into_iter().map(|i| i.0).collect();
        UuidArray {
            bitmap: Bitmap::ones(data.len()),
            data,
        }
    }
}

impl UuidArray {
    pub fn from_protobuf(array: &PbArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.get_values().len() == 1,
            "Must have only 1 buffer in array"
        );

        let buf = array.get_values()[0].get_body().as_slice();

        let mut builder = <UuidArrayBuilder>::new(cardinality);
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        let mut cursor = Cursor::new(buf);
        for not_null in bitmap.iter() {
            if not_null {
                let mut buf = [0u8; 16]; // Use explicit size instead of Uuid::size()
                cursor.read_exact(&mut buf)?;
                let item = <Uuid>::from_be_bytes(buf);
                builder.append(Some(item.as_scalar_ref()));
            } else {
                builder.append(None);
            }
        }
        let arr = builder.finish();
        ensure_eq!(arr.len(), cardinality);

        Ok(arr.into())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_uuid_array() {
        let uuids = [
            "123e4567-e89b-12d3-a456-426614174000",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
        .into_iter()
        .map(|s| Uuid::from_str(s).unwrap())
        .collect::<Vec<_>>();

        // Test creating array from iterator
        let array: UuidArray = uuids.clone().into_iter().collect();
        for (i, uuid) in uuids.iter().enumerate() {
            assert_eq!(
                crate::types::ScalarRef::to_owned_scalar(&array.value_at(i).unwrap()),
                *uuid
            );
        }

        // Test builder
        let mut builder = UuidArrayBuilder::new(3);
        for uuid in &uuids {
            builder.append(Some(uuid.as_scalar_ref()));
        }
        let array = builder.finish();

        assert_eq!(array.len(), 3);
        for (i, uuid) in uuids.iter().enumerate() {
            assert_eq!(
                crate::types::ScalarRef::to_owned_scalar(&array.value_at(i).unwrap()),
                *uuid
            );
        }
    }

    #[test]
    fn test_uuid_array_null() {
        let mut builder = UuidArrayBuilder::new(4);
        builder.append(Some(Uuid::nil().as_scalar_ref()));
        builder.append(None);
        builder.append(Some(Uuid::new_v4().as_scalar_ref()));
        builder.append(None);
        let array = builder.finish();

        assert_eq!(array.len(), 4);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));
        assert!(array.is_null(3));
    }

    #[test]
    fn test_uuid_array_protobuf() {
        let uuids = [
            "123e4567-e89b-12d3-a456-426614174000",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
        .into_iter()
        .map(|s| Uuid::from_str(s).unwrap())
        .collect::<Vec<_>>();

        let mut builder = UuidArrayBuilder::new(4);
        for uuid in &uuids {
            builder.append(Some(uuid.as_scalar_ref()));
        }
        builder.append(None);
        let array = builder.finish();

        // Convert to protobuf and back
        let proto = array.to_protobuf();
        let array2_impl = UuidArray::from_protobuf(&proto, 4).unwrap();

        // Verify array values are preserved
        // Convert ArrayImpl to UuidArray
        let array2 = match array2_impl {
            ArrayImpl::Uuid(uuid_array) => uuid_array,
            _ => panic!("Expected UUID array"),
        };
        assert_eq!(array2.len(), 4);
        for (i, uuid) in uuids.iter().enumerate() {
            assert_eq!(
                crate::types::ScalarRef::to_owned_scalar(&array2.value_at(i).unwrap()),
                *uuid
            );
        }
        assert!(array2.is_null(3));
    }
}
