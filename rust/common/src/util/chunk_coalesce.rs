use std::mem::swap;
use std::sync::Arc;

use crate::array::column::Column;
use crate::array::{ArrayBuilderImpl, ArrayImpl, DataChunk, RowRef};
use crate::error::Result;
use crate::types::DataTypeRef;

pub const DEFAULT_CHUNK_BUFFER_SIZE: usize = 2048;

/// A [`SlicedDataChunk`] is a [`DataChunk`] with offset.
pub struct SlicedDataChunk {
    data_chunk: DataChunk,
    offset: usize,
}

/// Used as a buffer for accumulating rows.
pub struct DataChunkBuilder {
    /// Data types for build array
    data_types: Vec<DataTypeRef>,
    batch_size: usize,

    /// Buffers storing current data
    array_builders: Vec<ArrayBuilderImpl>,
    buffered_count: usize,
}

impl DataChunkBuilder {
    pub fn new_with_default_size(data_types: Vec<DataTypeRef>) -> Self {
        Self::new(data_types, DEFAULT_CHUNK_BUFFER_SIZE)
    }

    pub fn new(data_types: Vec<DataTypeRef>, batch_size: usize) -> Self {
        Self {
            data_types,
            batch_size,
            array_builders: vec![],
            buffered_count: 0,
        }
    }

    /// Number of tuples left in one batch.
    #[inline(always)]
    fn left_buffer_count(&self) -> usize {
        self.batch_size - self.buffered_count
    }

    fn ensure_builders(&mut self) -> Result<()> {
        if self.array_builders.is_empty() {
            self.array_builders = self
                .data_types
                .iter()
                .map(|data_type| data_type.create_array_builder(self.batch_size))
                .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

            self.buffered_count = 0;
        }

        Ok(())
    }

    /// Returns not consumed input chunked data as sliced data chunk, and a data chunk of
    /// `batch_size`.
    ///
    /// If `input_chunk` is not totally consumed, it's returned with a new offset, which is equal to
    /// `old_offset + consumed_rows`. Otherwise the first value is `None`.
    ///
    /// If number of `batch_size` rows reached, it's returned as the second value of tuple.
    /// Otherwise it's `None`.
    pub fn append_chunk(
        &mut self,
        input_chunk: SlicedDataChunk,
    ) -> Result<(Option<SlicedDataChunk>, Option<DataChunk>)> {
        self.ensure_builders()?;

        let buffer_row_idx_iter = self.buffered_count..self.batch_size;
        let mut new_return_offset = input_chunk.offset;
        match input_chunk.data_chunk.visibility() {
            Some(vis) => {
                for vis in vis.iter_from(input_chunk.offset)? {
                    new_return_offset += 1;
                    if !vis {
                        continue;
                    }

                    self.append_one_row_internal(&input_chunk.data_chunk, new_return_offset - 1)?;
                    if self.buffered_count >= self.batch_size {
                        break;
                    }
                }
            }
            None => {
                (input_chunk.offset..input_chunk.data_chunk.capacity())
                    .zip(buffer_row_idx_iter)
                    .try_for_each(|(input_row_idx, _output_row_idx)| {
                        new_return_offset += 1;
                        self.append_one_row_internal(&input_chunk.data_chunk, input_row_idx)
                    })?;
            }
        }

        ensure!(self.buffered_count <= self.batch_size);

        let returned_input_chunk = if input_chunk.data_chunk.capacity() > new_return_offset {
            Some(input_chunk.with_new_offset_checked(new_return_offset)?)
        } else {
            None
        };

        let output_chunk = if self.buffered_count == self.batch_size {
            Some(self.build_data_chunk()?)
        } else {
            None
        };

        Ok((returned_input_chunk, output_chunk))
    }

    pub fn append_one_row<'a, I>(&mut self, row: I) -> Result<Option<DataChunk>>
    where
        I: IntoIterator<Item = Option<(&'a ArrayImpl, usize)>>,
    {
        ensure!(self.buffered_count < self.batch_size);
        self.ensure_builders()?;

        for (array_builder, column_opt) in self.array_builders.iter_mut().zip(row) {
            match column_opt {
                Some((array, row)) => array_builder.append_array_element(array, row)?,
                None => array_builder.append_null()?,
            }
        }

        self.buffered_count += 1;

        if self.buffered_count == self.batch_size {
            Ok(Some(self.build_data_chunk()?))
        } else {
            Ok(None)
        }
    }

    /// Returns all data in current buffer.
    ///
    /// If `buffered_count` is 0, `None` is returned.
    pub fn consume_all(&mut self) -> Result<Option<DataChunk>> {
        if self.buffered_count > 0 {
            self.build_data_chunk().map(Some)
        } else {
            Ok(None)
        }
    }

    fn append_one_row_internal(&mut self, data_chunk: &DataChunk, row_idx: usize) -> Result<()> {
        self.append_one_row_ref_impl(data_chunk.row_at(row_idx)?.0)
    }

    fn append_one_row_ref_impl(&mut self, row_ref: RowRef<'_>) -> Result<()> {
        self.array_builders
            .iter_mut()
            .zip(row_ref.0)
            .try_for_each(|(array_builder, scalar)| array_builder.append_datum_ref(scalar))?;
        self.buffered_count += 1;
        Ok(())
    }

    /// Used for append one row in some executors.
    /// Return a Some(data chunk) if the buffer is full after append one row.
    /// Otherwise None.
    pub fn append_one_row_ref(&mut self, row_ref: RowRef<'_>) -> Result<Option<DataChunk>> {
        ensure!(self.buffered_count < self.batch_size);
        self.ensure_builders()?;

        self.append_one_row_ref_impl(row_ref)?;
        if self.buffered_count == self.batch_size {
            Ok(Some(self.build_data_chunk()?))
        } else {
            Ok(None)
        }
    }

    fn build_data_chunk(&mut self) -> Result<DataChunk> {
        let mut new_array_builders = vec![];
        swap(&mut new_array_builders, &mut self.array_builders);
        self.buffered_count = 0;

        let columns = new_array_builders.into_iter().try_fold(
            Vec::with_capacity(self.data_types.len()),
            |mut vec, array_builder| -> Result<Vec<Column>> {
                let array = array_builder.finish()?;
                let column = Column::new(Arc::new(array));
                vec.push(column);
                Ok(vec)
            },
        )?;

        DataChunk::try_from(columns)
    }

    pub fn buffered_count(&self) -> usize {
        self.buffered_count
    }
}

impl SlicedDataChunk {
    pub fn new_checked(data_chunk: DataChunk) -> Result<Self> {
        SlicedDataChunk::with_offset_checked(data_chunk, 0)
    }

    pub fn with_offset_checked(data_chunk: DataChunk, offset: usize) -> Result<Self> {
        ensure!(offset < data_chunk.capacity());
        Ok(Self { data_chunk, offset })
    }

    pub fn with_new_offset_checked(self, new_offset: usize) -> Result<Self> {
        SlicedDataChunk::with_offset_checked(self.data_chunk, new_offset)
    }

    fn capacity(&self) -> usize {
        self.data_chunk.capacity() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::array::{DataChunk, I32Array, I64Array};
    use crate::buffer::Bitmap;
    use crate::column;
    use crate::types::{Int32Type, Int64Type};
    use crate::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};

    #[test]
    fn test_append_chunk() {
        let mut builder = DataChunkBuilder::new(
            vec![
                Arc::new(Int32Type::new(true)),
                Arc::new(Int64Type::new(true)),
            ],
            3,
        );

        // Append a chunk with 2 rows
        let input = {
            let column1 = column! {
              I32Array, [Some(3), None]
            };

            let column2 = column! {
              I64Array, [None, Some(7i64)]
            };

            let chunk =
                DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
            SlicedDataChunk::new_checked(chunk).expect("Failed to create sliced data chunk")
        };

        let (returned_input, output) = builder
            .append_chunk(input)
            .expect("Failed to append chunk!");
        assert!(returned_input.is_none());
        assert!(output.is_none());

        // Append a chunk with 4 rows
        let input = {
            let column1 = column! {I32Array, [Some(3), None, Some(4), None]};

            let column2 = column! {I64Array, [None, Some(7i64), Some(8i64), Some(9i64)]};

            let chunk =
                DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
            SlicedDataChunk::new_checked(chunk).expect("Failed to create sliced data chunk")
        };
        let (returned_input, output) = builder
            .append_chunk(input)
            .expect("Failed to append chunk!");
        assert_eq!(Some(1), returned_input.as_ref().map(|c| c.offset));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::cardinality));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::capacity));
        assert!(output.unwrap().visibility().is_none());

        // Append last input
        let (returned_input, output) = builder
            .append_chunk(returned_input.unwrap())
            .expect("Failed to append chunk!");
        assert!(returned_input.is_none());
        assert_eq!(Some(3), output.as_ref().map(DataChunk::cardinality));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::capacity));
        assert!(output.unwrap().visibility().is_none());
    }

    #[test]
    fn test_append_chunk_with_bitmap() {
        let mut builder = DataChunkBuilder::new(
            vec![
                Arc::new(Int32Type::new(true)),
                Arc::new(Int64Type::new(true)),
            ],
            3,
        );

        // Append a chunk with 2 rows
        let input = {
            let column1 = column! {I32Array, [Some(3), None]};
            let column2 = column! {I64Array, [None, Some(7i64)]};

            let chunk =
                DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
            let bitmap = Bitmap::try_from(vec![true, false]).expect("Failed to create bitmap");
            SlicedDataChunk::new_checked(chunk.with_visibility(bitmap))
                .expect("Failed to create sliced data chunk")
        };

        let (returned_input, output) = builder
            .append_chunk(input)
            .expect("Failed to append chunk!");
        assert!(returned_input.is_none());
        assert!(output.is_none());
        assert_eq!(1, builder.buffered_count());

        // Append a chunk with 4 rows
        let input = {
            let column1 = column! { I32Array, [Some(3), None, Some(4), None] };

            let column2 = column! { I64Array, [None, Some(7i64), Some(8i64), Some(9i64)]};

            let chunk =
                DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
            let bitmap =
                Bitmap::try_from(vec![false, true, true, false]).expect("Failed to create bitmap!");
            SlicedDataChunk::new_checked(chunk.with_visibility(bitmap))
                .expect("Failed to create sliced data chunk")
        };
        let (returned_input, output) = builder
            .append_chunk(input)
            .expect("Failed to append chunk!");
        assert_eq!(Some(3), returned_input.as_ref().map(|c| c.offset));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::cardinality));
        assert_eq!(Some(3), output.as_ref().map(DataChunk::capacity));
        assert!(output.unwrap().visibility().is_none());
        assert_eq!(0, builder.buffered_count());

        // Append last input
        let (returned_input, output) = builder
            .append_chunk(returned_input.unwrap())
            .expect("Failed to append chunk!");
        assert!(returned_input.is_none());
        assert!(output.is_none());
        assert_eq!(0, builder.buffered_count());
    }

    #[test]
    fn test_consume_all() {
        let mut builder = DataChunkBuilder::new(
            vec![
                Arc::new(Int32Type::new(true)),
                Arc::new(Int64Type::new(true)),
            ],
            3,
        );

        // It should return `None` when builder is empty
        assert!(builder.consume_all().unwrap().is_none());

        // Append a chunk with 2 rows
        let input = {
            let column1 = column! {I32Array, [Some(3), None]};
            let column2 = column! {I64Array, [None, Some(7i64)] };
            let chunk =
                DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
            SlicedDataChunk::new_checked(chunk).expect("Failed to create sliced data chunk")
        };

        let (returned_input, output) = builder
            .append_chunk(input)
            .expect("Failed to append chunk!");
        assert!(returned_input.is_none());
        assert!(output.is_none());

        let output = builder.consume_all().expect("Failed to consume all!");
        assert!(output.is_some());
        assert_eq!(Some(2), output.as_ref().map(DataChunk::cardinality));
        assert_eq!(Some(2), output.as_ref().map(DataChunk::capacity));
        assert!(output.unwrap().visibility().is_none());
    }
}
