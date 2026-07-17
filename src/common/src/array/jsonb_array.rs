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

use std::mem::size_of;

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::{PbArray, PbArrayType};

use super::{Array, ArrayBuilder, ArrayError, ArrayImpl, ArrayResult};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, JsonbRef, JsonbVal, Scalar};
use crate::util::iter_util::ZipEqDebug;

#[derive(Debug, Clone)]
pub struct JsonbArrayBuilder {
    bitmap: BitmapBuilder,
    /// Each row is an independent JSONB value. In particular, the internal offsets of a JSONB
    /// value are never shared by multiple rows.
    data: Vec<Option<JsonbVal>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonbArray {
    bitmap: Bitmap,
    data: Box<[Option<JsonbVal>]>,
}

impl ArrayBuilder for JsonbArrayBuilder {
    type ArrayType = JsonbArray;

    fn new(capacity: usize) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, DataType::Jsonb);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        if n == 0 {
            return;
        }
        self.bitmap.append_n(n, value.is_some());
        self.data
            .extend(std::iter::repeat_n(value.map(JsonbVal::from), n));
    }

    fn append_owned(&mut self, value: Option<<Self::ArrayType as Array>::OwnedItem>) {
        self.bitmap.append(value.is_some());
        self.data.push(value);
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        self.bitmap.append_bitmap(&other.bitmap);
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.bitmap.pop()?;
        self.data.pop().unwrap();
        Some(())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> Self::ArrayType {
        Self::ArrayType {
            bitmap: self.bitmap.finish(),
            data: self.data.into_boxed_slice(),
        }
    }
}

impl EstimateSize for JsonbArrayBuilder {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size()
            + self.data.capacity() * size_of::<Option<JsonbVal>>()
            + self
                .data
                .iter()
                .map(EstimateSize::estimated_heap_size)
                .sum::<usize>()
    }
}

impl JsonbArrayBuilder {
    pub fn writer(&mut self) -> JsonbWriter<'_> {
        JsonbWriter::new(self)
    }
}

impl JsonbArray {
    /// Loads a `JsonbArray` from a protobuf array.
    ///
    /// The two-buffer encoding stores independent JSONB values. The one-buffer encoding is kept
    /// for compatibility with payloads produced when the whole column was one JSONB array.
    ///
    /// See also `JsonbArray::to_protobuf`.
    pub fn from_protobuf(array: &PbArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        ensure_eq!(bitmap.len(), cardinality);

        let data = match array.values.as_slice() {
            [legacy] => Self::decode_legacy_values(&legacy.body, &bitmap, cardinality)?,
            [offsets, values] => Self::decode_values(&offsets.body, &values.body, &bitmap)?,
            _ => {
                return Err(ArrayError::internal(
                    "Must have exactly 1 or 2 buffers in a jsonb array",
                ));
            }
        };

        Ok(JsonbArray { bitmap, data }.into())
    }

    fn decode_legacy_values(
        encoded: &[u8],
        bitmap: &Bitmap,
        cardinality: usize,
    ) -> ArrayResult<Box<[Option<JsonbVal>]>> {
        let value = jsonbb::Value::from_bytes(encoded);
        let values = value
            .as_array()
            .ok_or_else(|| ArrayError::internal("legacy jsonb array is not an array"))?;
        ensure_eq!(values.len(), cardinality);

        Ok(bitmap
            .iter()
            .zip_eq_debug(values.iter())
            .map(|(not_null, value)| not_null.then(|| JsonbVal::from(JsonbRef(value))))
            .collect())
    }

    fn decode_values(
        encoded_offsets: &[u8],
        encoded_values: &[u8],
        bitmap: &Bitmap,
    ) -> ArrayResult<Box<[Option<JsonbVal>]>> {
        let expected_offset_count = bitmap.count_ones() + 1;
        ensure_eq!(
            encoded_offsets.len(),
            expected_offset_count * size_of::<u64>()
        );

        let mut offsets = encoded_offsets.chunks_exact(size_of::<u64>()).map(|bytes| {
            let bytes: [u8; size_of::<u64>()] = bytes.try_into().unwrap();
            u64::from_be_bytes(bytes)
        });
        let mut previous = offsets.next().unwrap();
        ensure_eq!(previous, 0);

        let mut data = Vec::with_capacity(bitmap.len());
        for not_null in bitmap.iter() {
            if !not_null {
                data.push(None);
                continue;
            }

            let next = offsets.next().unwrap();
            ensure!(previous <= next, "jsonb offsets must be non-decreasing");
            let start = usize::try_from(previous)
                .map_err(|_| ArrayError::internal("jsonb offset does not fit usize"))?;
            let end = usize::try_from(next)
                .map_err(|_| ArrayError::internal("jsonb offset does not fit usize"))?;
            ensure!(end <= encoded_values.len(), "jsonb offset is out of bounds");
            data.push(Some(JsonbVal::from(jsonbb::Value::from_bytes(
                &encoded_values[start..end],
            ))));
            previous = next;
        }

        ensure!(offsets.next().is_none(), "too many jsonb offsets");
        let final_offset = usize::try_from(previous)
            .map_err(|_| ArrayError::internal("jsonb offset does not fit usize"))?;
        ensure_eq!(final_offset, encoded_values.len());
        Ok(data.into_boxed_slice())
    }
}

impl EstimateSize for JsonbArray {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size()
            + self.data.len() * size_of::<Option<JsonbVal>>()
            + self
                .data
                .iter()
                .map(EstimateSize::estimated_heap_size)
                .sum::<usize>()
    }
}

impl Array for JsonbArray {
    type Builder = JsonbArrayBuilder;
    type OwnedItem = JsonbVal;
    type RefItem<'a> = JsonbRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        match unsafe { self.data.get_unchecked(idx) } {
            Some(value) => value.as_scalar_ref(),
            None => JsonbRef::null(),
        }
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn to_protobuf(&self) -> PbArray {
        // Each non-null value is encoded separately. This prevents jsonbb's per-value offset limit
        // from applying to the whole column.
        use risingwave_pb::common::Buffer;
        use risingwave_pb::common::buffer::CompressionType;

        let mut offset_buffer =
            Vec::with_capacity((self.bitmap.count_ones() + 1) * size_of::<u64>());
        let mut data_buffer = Vec::new();
        let jsonb_null = JsonbVal::null();

        for (value, not_null) in self.data.iter().zip_eq_debug(self.bitmap.iter()) {
            if !not_null {
                continue;
            }
            offset_buffer.extend_from_slice(&(data_buffer.len() as u64).to_be_bytes());
            data_buffer.extend_from_slice(value.as_ref().unwrap_or(&jsonb_null).0.as_bytes());
        }
        offset_buffer.extend_from_slice(&(data_buffer.len() as u64).to_be_bytes());

        PbArray {
            null_bitmap: Some(self.null_bitmap().to_protobuf()),
            values: vec![
                Buffer {
                    compression: CompressionType::None as i32,
                    body: offset_buffer,
                },
                Buffer {
                    compression: CompressionType::None as i32,
                    body: data_buffer,
                },
            ],
            array_type: PbArrayType::Jsonb as i32,
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
        DataType::Jsonb
    }
}

impl FromIterator<Option<JsonbVal>> for JsonbArray {
    fn from_iter<I: IntoIterator<Item = Option<JsonbVal>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for value in iter {
            builder.append_owned(value);
        }
        builder.finish()
    }
}

impl FromIterator<JsonbVal> for JsonbArray {
    fn from_iter<I: IntoIterator<Item = JsonbVal>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

/// Builds one independent JSONB value for a row. Dropping or rolling back an unfinished writer
/// simply discards that value and cannot affect previously appended rows.
pub struct JsonbWriter<'a> {
    array_builder: &'a mut JsonbArrayBuilder,
    builder: jsonbb::Builder,
}

impl JsonbWriter<'_> {
    pub fn new(array_builder: &mut JsonbArrayBuilder) -> JsonbWriter<'_> {
        JsonbWriter {
            array_builder,
            builder: jsonbb::Builder::<Vec<u8>>::new(),
        }
    }

    pub fn inner(&mut self) -> &mut jsonbb::Builder {
        &mut self.builder
    }

    /// Commits the completed value as one row.
    pub fn finish(self) {
        let JsonbWriter {
            array_builder,
            builder,
        } = self;
        array_builder.append_owned(Some(JsonbVal::from(builder.finish())));
    }

    /// Discards the partially built value.
    pub fn rollback(self) {}
}

#[cfg(test)]
mod tests {
    use risingwave_pb::common::Buffer;
    use risingwave_pb::common::buffer::CompressionType;

    use super::*;

    fn json(value: &str) -> JsonbVal {
        value.parse().unwrap()
    }

    #[test]
    fn test_independent_values_and_protobuf_roundtrip() {
        let array = JsonbArray::from_iter([
            Some(json(r#"{"a": 1}"#)),
            None,
            Some(JsonbVal::null()),
            Some(json(r#"[1, 2, 3]"#)),
        ]);

        assert_eq!(array.len(), 4);
        assert!(array.value_at(1).is_none());
        assert!(array.value_at(2).unwrap().is_jsonb_null());

        let protobuf = array.to_protobuf();
        assert_eq!(protobuf.values.len(), 2);
        assert_eq!(protobuf.values[0].body.len(), 4 * size_of::<u64>());

        let decoded = JsonbArray::from_protobuf(&protobuf, array.len()).unwrap();
        assert_eq!(decoded.as_jsonb(), &array);
    }

    #[test]
    fn test_decode_legacy_single_jsonb_array() {
        let expected =
            JsonbArray::from_iter([Some(json(r#"{"a": 1}"#)), None, Some(JsonbVal::null())]);

        let mut legacy_builder = jsonbb::Builder::<Vec<u8>>::new();
        legacy_builder.begin_array();
        for value in &expected.data {
            match value {
                Some(value) => legacy_builder.add_value(value.as_scalar_ref().0),
                None => legacy_builder.add_null(),
            }
        }
        legacy_builder.end_array();

        let protobuf = PbArray {
            null_bitmap: Some(expected.null_bitmap().to_protobuf()),
            values: vec![Buffer {
                compression: CompressionType::None as i32,
                body: legacy_builder.finish().as_bytes().to_vec(),
            }],
            array_type: PbArrayType::Jsonb as i32,
            struct_array_data: None,
            list_array_data: None,
        };

        let decoded = JsonbArray::from_protobuf(&protobuf, expected.len()).unwrap();
        assert_eq!(decoded.as_jsonb(), &expected);
    }

    #[test]
    fn test_writer_commit_and_rollback_are_isolated() {
        let mut builder = JsonbArrayBuilder::new(3);
        {
            let mut writer = builder.writer();
            writer.inner().add_string("committed");
            writer.finish();
        }
        {
            let mut writer = builder.writer();
            writer.inner().add_string("rolled back");
            writer.rollback();
        }
        builder.append_null();
        {
            let mut writer = builder.writer();
            writer.inner().add_string("dropped");
        }

        let array = builder.finish();
        assert_eq!(array.len(), 2);
        assert_eq!(
            array.value_at(0).unwrap(),
            json(r#""committed""#).as_scalar_ref()
        );
        assert!(array.value_at(1).is_none());
    }
}
