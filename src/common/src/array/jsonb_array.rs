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

use super::{
    Array, ArrayBuilder, ArrayError, ArrayImpl, ArrayResult, BytesArray, BytesArrayBuilder,
};
use crate::bitmap::Bitmap;
use crate::types::{DataType, JsonbRef, JsonbVal};

#[derive(Debug, Clone, EstimateSize)]
pub struct JsonbArrayBuilder {
    bytes: BytesArrayBuilder,
}

#[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
pub struct JsonbArray {
    /// Each row is an independently encoded JSONB value in a contiguous byte buffer. In
    /// particular, the internal offsets of a JSONB value are never shared by multiple rows.
    bytes: BytesArray,
}

impl ArrayBuilder for JsonbArrayBuilder {
    type ArrayType = JsonbArray;

    fn new(capacity: usize) -> Self {
        Self {
            bytes: BytesArrayBuilder::new(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, DataType::Jsonb);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        match value {
            Some(value) => {
                for _ in 0..n {
                    self.append_value(value);
                }
            }
            None => self.bytes.append_n(n, None),
        }
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        self.bytes.append_array(&other.bytes);
    }

    fn pop(&mut self) -> Option<()> {
        self.bytes.pop()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn finish(self) -> Self::ArrayType {
        Self::ArrayType {
            bytes: self.bytes.finish(),
        }
    }
}

impl JsonbArrayBuilder {
    pub fn writer(&mut self) -> JsonbWriter<'_> {
        JsonbWriter::new(self)
    }

    /// Appends the standalone jsonbb encoding of `value`.
    ///
    /// A jsonbb value is encoded as its data followed by a four-byte root entry. `to_raw_parts`
    /// rebuilds that entry relative to the returned data slice, which also makes a nested
    /// `JsonbRef` independent from the value it was extracted from.
    fn append_value(&mut self, value: JsonbRef<'_>) {
        let (entry, data) = value.0.to_raw_parts();
        let mut writer = self.bytes.writer();
        writer.write_ref(data);
        writer.write_ref(entry.as_bytes());
        writer.finish();
    }

    fn append_encoded(&mut self, encoded: &[u8]) {
        self.bytes.append(Some(encoded));
    }
}

impl JsonbArray {
    /// Loads a `JsonbArray` from a protobuf array.
    ///
    /// See also `JsonbArray::to_protobuf`.
    pub fn from_protobuf(array: &PbArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
        let bitmap: Bitmap = array.get_null_bitmap()?.into();
        ensure_eq!(bitmap.len(), cardinality);
        ensure_eq!(array.values.len(), 2);

        Self::decode_values(&array.values[0].body, &array.values[1].body, &bitmap)
    }

    fn decode_values(
        encoded_offsets: &[u8],
        encoded_values: &[u8],
        bitmap: &Bitmap,
    ) -> ArrayResult<ArrayImpl> {
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

        let mut builder = JsonbArrayBuilder::new(bitmap.len());
        for not_null in bitmap.iter() {
            if !not_null {
                builder.append_null();
                continue;
            }

            let next = offsets.next().unwrap();
            ensure!(previous <= next, "jsonb offsets must be non-decreasing");
            let start = usize::try_from(previous)
                .map_err(|_| ArrayError::internal("jsonb offset does not fit usize"))?;
            let end = usize::try_from(next)
                .map_err(|_| ArrayError::internal("jsonb offset does not fit usize"))?;
            ensure!(end <= encoded_values.len(), "jsonb offset is out of bounds");
            ensure!(end - start >= size_of::<u32>(), "jsonb value is too short");
            builder.append_encoded(&encoded_values[start..end]);
            previous = next;
        }

        ensure!(offsets.next().is_none(), "too many jsonb offsets");
        let final_offset = usize::try_from(previous)
            .map_err(|_| ArrayError::internal("jsonb offset does not fit usize"))?;
        ensure_eq!(final_offset, encoded_values.len());
        Ok(builder.finish().into())
    }
}

impl Array for JsonbArray {
    type Builder = JsonbArrayBuilder;
    type OwnedItem = JsonbVal;
    type RefItem<'a> = JsonbRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        let encoded = unsafe { self.bytes.raw_value_at_unchecked(idx) };
        if encoded.is_empty() {
            // The raw value for a SQL NULL is the default JSONB value.
            JsonbRef::null()
        } else {
            JsonbRef(jsonbb::ValueRef::from_bytes(encoded))
        }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn to_protobuf(&self) -> PbArray {
        // The byte-array offsets delimit independently encoded JSONB values. Therefore jsonbb's
        // per-value offset limit does not apply to the whole column.
        PbArray {
            array_type: PbArrayType::Jsonb as i32,
            ..self.bytes.to_protobuf()
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        self.bytes.null_bitmap()
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bytes.into_null_bitmap()
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bytes.set_bitmap(bitmap);
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
        let value = builder.finish();
        array_builder.append_value(JsonbRef(value.as_ref()));
    }

    /// Discards the partially built value.
    pub fn rollback(self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Scalar;

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
        assert_eq!(
            array.value_at(0).unwrap(),
            json(r#"{"a": 1}"#).as_scalar_ref()
        );
        assert!(array.value_at(1).is_none());
        assert!(array.value_at(2).unwrap().is_jsonb_null());
        assert_eq!(
            array.value_at(3).unwrap(),
            json(r#"[1, 2, 3]"#).as_scalar_ref()
        );

        let protobuf = array.to_protobuf();
        assert_eq!(protobuf.values.len(), 2);
        assert_eq!(protobuf.values[0].body.len(), 4 * size_of::<u64>());

        let decoded = JsonbArray::from_protobuf(&protobuf, array.len()).unwrap();
        assert_eq!(decoded.as_jsonb(), &array);
    }

    #[test]
    fn test_append_value_matches_jsonbb_encoding() {
        let values = [
            json("null"),
            json("true"),
            json("42"),
            json(r#""text""#),
            json("[1, 2, 3]"),
            json(r#"{"a": [1, true]}"#),
        ];
        let expected = values
            .iter()
            .flat_map(|value| value.0.as_bytes())
            .copied()
            .collect::<Vec<_>>();

        let array = JsonbArray::from_iter(values);
        assert_eq!(array.to_protobuf().values[1].body, expected);
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
