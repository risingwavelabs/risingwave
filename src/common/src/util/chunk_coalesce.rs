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

use std::iter::FusedIterator;
use std::mem::swap;

use super::iter_util::ZipEqDebug;
use crate::array::{ArrayBuilderImpl, ArrayImpl, ArrayRef, DataChunk};
use crate::row::Row;
use crate::types::{DataType, ToDatumRef};

/// A [`SlicedDataChunk`] is a [`DataChunk`] with offset.
pub struct SlicedDataChunk {
    data_chunk: DataChunk,
    offset: usize,
}

/// Used as a buffer for accumulating rows.
pub struct DataChunkBuilder {
    /// Data types for build array
    data_types: Vec<DataType>,
    batch_size: usize,

    /// Buffers storing current data
    array_builders: Vec<ArrayBuilderImpl>,
    buffered_count: usize,
}

impl DataChunkBuilder {
    pub fn new(data_types: Vec<DataType>, batch_size: usize) -> Self {
        Self {
            data_types,
            batch_size,
            array_builders: vec![],
            buffered_count: 0,
        }
    }

    /// Lazily create the array builders if absent
    fn ensure_builders(&mut self) {
        if self.array_builders.len() != self.data_types.len() {
            self.array_builders = self
                .data_types
                .iter()
                .map(|data_type| data_type.create_array_builder(self.batch_size))
                .collect::<Vec<ArrayBuilderImpl>>();

            assert!(self.buffered_count == 0);
        }
    }

    /// Returns not consumed input chunked data as sliced data chunk, and a data chunk of
    /// `batch_size`.
    ///
    /// If `input_chunk` is not totally consumed, it's returned with a new offset, which is equal to
    /// `old_offset + consumed_rows`. Otherwise the first value is `None`.
    ///
    /// If number of `batch_size` rows reached, it's returned as the second value of tuple.
    /// Otherwise it's `None`.
    #[must_use]
    fn append_chunk_inner(
        &mut self,
        input_chunk: SlicedDataChunk,
    ) -> (Option<SlicedDataChunk>, Option<DataChunk>) {
        self.ensure_builders();

        let mut new_return_offset = input_chunk.offset;
        match input_chunk.data_chunk.visibility() {
            Some(vis) => {
                for vis in vis.iter().skip(input_chunk.offset) {
                    new_return_offset += 1;
                    if !vis {
                        continue;
                    }

                    self.append_one_row_internal(&input_chunk.data_chunk, new_return_offset - 1);
                    if self.buffered_count >= self.batch_size {
                        break;
                    }
                }
            }
            None => {
                let num_rows_to_append = std::cmp::min(
                    self.batch_size - self.buffered_count,
                    input_chunk.data_chunk.capacity() - input_chunk.offset,
                );
                let end_offset = input_chunk.offset + num_rows_to_append;
                for input_row_idx in input_chunk.offset..end_offset {
                    new_return_offset += 1;
                    self.append_one_row_internal(&input_chunk.data_chunk, input_row_idx)
                }
            }
        }

        assert!(self.buffered_count <= self.batch_size);

        let returned_input_chunk = if input_chunk.data_chunk.capacity() > new_return_offset {
            Some(input_chunk.with_new_offset_checked(new_return_offset))
        } else {
            None
        };

        let output_chunk = if self.buffered_count == self.batch_size {
            Some(self.build_data_chunk())
        } else {
            None
        };

        (returned_input_chunk, output_chunk)
    }

    /// Returns an iterator that yields data chunks during appending the whole input data chunk. The
    /// iterator must be fully consumed to ensure all data is appended.
    #[must_use]
    pub fn append_chunk(&mut self, data_chunk: DataChunk) -> AppendDataChunk<'_> {
        AppendDataChunk {
            builder: self,
            remaining: Some(SlicedDataChunk::new_checked(data_chunk)),
        }
    }

    /// Returns all data in current buffer.
    ///
    /// If `buffered_count` is 0, `None` is returned.
    #[must_use]
    pub fn consume_all(&mut self) -> Option<DataChunk> {
        if self.buffered_count > 0 {
            Some(self.build_data_chunk())
        } else {
            None
        }
    }

    fn append_one_row_internal(&mut self, data_chunk: &DataChunk, row_idx: usize) {
        self.do_append_one_row_from_datums(data_chunk.row_at(row_idx).0.iter());
    }

    fn do_append_one_row_from_datums(&mut self, datums: impl Iterator<Item = impl ToDatumRef>) {
        for (array_builder, datum) in self.array_builders.iter_mut().zip_eq_debug(datums) {
            array_builder.append_datum(datum);
        }
        self.buffered_count += 1;
    }

    /// Append one row from the given [`Row`].
    /// Return a data chunk if the buffer is full after append one row. Otherwise `None`.
    #[must_use]
    pub fn append_one_row(&mut self, row: impl Row) -> Option<DataChunk> {
        assert!(self.buffered_count < self.batch_size);
        self.ensure_builders();

        self.do_append_one_row_from_datums(row.iter());
        if self.buffered_count == self.batch_size {
            Some(self.build_data_chunk())
        } else {
            None
        }
    }

    /// Append one row from the given two arrays.
    /// Return a data chunk if the buffer is full after append one row. Otherwise `None`.
    #[must_use]
    pub fn append_one_row_from_array_elements<'a, I1, I2>(
        &mut self,
        left_arrays: I1,
        left_row_id: usize,
        right_arrays: I2,
        right_row_id: usize,
    ) -> Option<DataChunk>
    where
        I1: Iterator<Item = &'a ArrayImpl>,
        I2: Iterator<Item = &'a ArrayImpl>,
    {
        assert!(self.buffered_count < self.batch_size);
        self.ensure_builders();

        for (array_builder, (array, row_id)) in self.array_builders.iter_mut().zip_eq_debug(
            left_arrays
                .map(|array| (array, left_row_id))
                .chain(right_arrays.map(|array| (array, right_row_id))),
        ) {
            array_builder.append_array_element(array, row_id)
        }

        self.buffered_count += 1;

        if self.buffered_count == self.batch_size {
            Some(self.build_data_chunk())
        } else {
            None
        }
    }

    fn build_data_chunk(&mut self) -> DataChunk {
        let mut new_array_builders = vec![];
        swap(&mut new_array_builders, &mut self.array_builders);
        let cardinality = self.buffered_count;
        self.buffered_count = 0;

        let columns = new_array_builders.into_iter().fold(
            Vec::with_capacity(self.data_types.len()),
            |mut vec, array_builder| -> Vec<ArrayRef> {
                let column = array_builder.finish().into();
                vec.push(column);
                vec
            },
        );
        DataChunk::new(columns, cardinality)
    }

    pub fn buffered_count(&self) -> usize {
        self.buffered_count
    }

    pub fn data_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}

impl Drop for DataChunkBuilder {
    fn drop(&mut self) {
        // Possible to fail when async task gets cancelled.
        if self.buffered_count != 0 {
            tracing::warn!(
                remaining = self.buffered_count,
                "dropping non-empty data chunk builder"
            );
        }
    }
}

/// The iterator that yields data chunks during appending a data chunk to a [`DataChunkBuilder`].
pub struct AppendDataChunk<'a> {
    builder: &'a mut DataChunkBuilder,
    remaining: Option<SlicedDataChunk>,
}

impl Iterator for AppendDataChunk<'_> {
    type Item = DataChunk;

    fn next(&mut self) -> Option<Self::Item> {
        let (remaining, output) = self.builder.append_chunk_inner(self.remaining.take()?);
        self.remaining = remaining;
        output
    }
}

impl FusedIterator for AppendDataChunk<'_> {}

impl Drop for AppendDataChunk<'_> {
    fn drop(&mut self) {
        // Possible to fail when async task gets cancelled.
        if self.remaining.is_some() {
            tracing::warn!("dropping `AppendDataChunk` without exhausting it");
        }
    }
}

impl SlicedDataChunk {
    pub fn new_checked(data_chunk: DataChunk) -> Self {
        SlicedDataChunk::with_offset_checked(data_chunk, 0)
    }

    pub fn with_offset_checked(data_chunk: DataChunk, offset: usize) -> Self {
        assert!(offset < data_chunk.capacity());
        Self { data_chunk, offset }
    }

    pub fn with_new_offset_checked(self, new_offset: usize) -> Self {
        SlicedDataChunk::with_offset_checked(self.data_chunk, new_offset)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::array::DataChunk;
    use crate::test_prelude::DataChunkTestExt;
    use crate::types::{DataType, ScalarImpl};
    use crate::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};

    #[test]
    fn test_append_chunk() {
        let mut builder = DataChunkBuilder::new(vec![DataType::Int32, DataType::Int64], 3);

        // Append a chunk with 2 rows
        let input = SlicedDataChunk::new_checked(DataChunk::from_pretty(
            "i I
             3 .
             . 7",
        ));

        let (returned_input, output) = builder.append_chunk_inner(input);
        assert!(returned_input.is_none());
        assert!(output.is_none());

        // Append a chunk with 4 rows
        let input = SlicedDataChunk::new_checked(DataChunk::from_pretty(
            "i I
             3 .
             . 7
             4 8
             . 9",
        ));
        let (returned_input, output) = builder.append_chunk_inner(input);
        assert_eq!(Some(1), returned_input.as_ref().map(|c| c.offset));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::cardinality));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::capacity));
        assert!(output.unwrap().visibility().is_none());

        // Append last input
        let (returned_input, output) = builder.append_chunk_inner(returned_input.unwrap());
        assert!(returned_input.is_none());
        assert_eq!(Some(3), output.as_ref().map(DataChunk::cardinality));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::capacity));
        assert!(output.unwrap().visibility().is_none());
    }

    #[test]
    fn test_append_chunk_with_bitmap() {
        let mut builder = DataChunkBuilder::new(vec![DataType::Int32, DataType::Int64], 3);

        // Append a chunk with 2 rows
        let input = SlicedDataChunk::new_checked(DataChunk::from_pretty(
            "i I
             3 .
             . 7 D",
        ));

        let (returned_input, output) = builder.append_chunk_inner(input);
        assert!(returned_input.is_none());
        assert!(output.is_none());
        assert_eq!(1, builder.buffered_count());

        // Append a chunk with 4 rows
        let input = SlicedDataChunk::new_checked(DataChunk::from_pretty(
            "i I
             3 . D
             . 7
             4 8
             . 9 D",
        ));
        let (returned_input, output) = builder.append_chunk_inner(input);
        assert_eq!(Some(3), returned_input.as_ref().map(|c| c.offset));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::cardinality));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::capacity));
        assert!(output.unwrap().visibility().is_none());
        assert_eq!(0, builder.buffered_count());

        // Append last input
        let (returned_input, output) = builder.append_chunk_inner(returned_input.unwrap());
        assert!(returned_input.is_none());
        assert!(output.is_none());
        assert_eq!(0, builder.buffered_count());
    }

    #[test]
    fn test_append_chunk_iter() {
        let mut builder = DataChunkBuilder::new(vec![DataType::Int32, DataType::Int64], 3);

        // Append a chunk with 2 rows
        let input = DataChunk::from_pretty(
            "i I
             3 .
             . 7",
        );

        let outputs = builder.append_chunk(input).collect_vec();
        assert!(outputs.is_empty());

        // Append a chunk with 4 rows
        let input = DataChunk::from_pretty(
            "i I
             3 .
             . 7
             4 8
             . 9",
        );

        let [output_1, output_2]: [_; 2] = builder
            .append_chunk(input)
            .collect_vec()
            .try_into()
            .unwrap();

        for output in &[output_1, output_2] {
            assert_eq!(3, output.cardinality());
            assert_eq!(3, output.capacity());
            assert!(output.visibility().is_none());
        }
    }

    #[test]
    fn test_consume_all() {
        let mut builder = DataChunkBuilder::new(vec![DataType::Int32, DataType::Int64], 3);

        // It should return `None` when builder is empty
        assert!(builder.consume_all().is_none());

        // Append a chunk with 2 rows
        let input = SlicedDataChunk::new_checked(DataChunk::from_pretty(
            "i I
             3 .
             . 7",
        ));

        let (returned_input, output) = builder.append_chunk_inner(input);
        assert!(returned_input.is_none());
        assert!(output.is_none());

        let output = builder.consume_all().expect("Failed to consume all!");
        assert_eq!(2, output.cardinality());
        assert_eq!(2, output.capacity());
        assert!(output.visibility().is_none());
    }

    #[test]
    fn test_append_one_row_from_array_elements() {
        let mut builder = DataChunkBuilder::new(vec![DataType::Int32, DataType::Int64], 3);

        assert!(builder.consume_all().is_none());

        let mut left_array_builder = DataType::Int32.create_array_builder(5);
        for v in [1, 2, 3, 4, 5] {
            left_array_builder.append_datum(&Some(ScalarImpl::Int32(v)));
        }
        let left_arrays = vec![left_array_builder.finish()];

        let mut right_array_builder = DataType::Int64.create_array_builder(5);
        for v in [5, 4, 3, 2, 1] {
            right_array_builder.append_datum(&Some(ScalarImpl::Int64(v)));
        }
        let right_arrays = vec![right_array_builder.finish()];

        let mut output_chunks = Vec::new();

        for i in 0..5 {
            if let Some(chunk) = builder.append_one_row_from_array_elements(
                left_arrays.iter(),
                i,
                right_arrays.iter(),
                i,
            ) {
                output_chunks.push(chunk)
            }
        }

        if let Some(chunk) = builder.consume_all() {
            output_chunks.push(chunk)
        }

        assert_eq!(
            output_chunks,
            vec![
                DataChunk::from_pretty(
                    "i I
                    1 5
                    2 4
                    3 3"
                ),
                DataChunk::from_pretty(
                    "i I
                    4 2
                    5 1"
                ),
            ]
        )
    }
}
