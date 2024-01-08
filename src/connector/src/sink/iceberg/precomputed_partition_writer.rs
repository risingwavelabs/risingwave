// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use arrow_array::{BooleanArray, RecordBatch, StructArray};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{DataType, SchemaRef};
use arrow_select::filter::filter_record_batch;
use icelake::io_v2::{IcebergWriteResult, IcebergWriter, IcebergWriterBuilder};
use icelake::types::{struct_to_anyvalue_array_with_type, Any, AnyValue};
use risingwave_common::util::iter_util::ZipEqFast;

#[derive(Clone)]
pub struct PrecomputedPartitionedWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    partition_type: Any,
    partition_col_idx: usize,
}

impl<B: IcebergWriterBuilder> PrecomputedPartitionedWriterBuilder<B> {
    pub fn new(inner: B, partition_type: Any, partition_col_idx: usize) -> Self {
        Self {
            inner,
            partition_type,
            partition_col_idx,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for PrecomputedPartitionedWriterBuilder<B> {
    type R = PrecomputedPartitionedWriter<B>;

    async fn build(self, schema: &SchemaRef) -> icelake::Result<Self::R> {
        let mut new_schema = schema.as_ref().clone();
        let remove_field = new_schema.remove(self.partition_col_idx);

        // Check the remove type is the same as the partition type.
        let expect_partition_type =
            self.partition_type.clone().try_into().map_err(|e| {
                icelake::Error::new(icelake::ErrorKind::ArrowError, format!("{}", e))
            })?;
        let is_same = if let DataType::Struct(fields) = remove_field.data_type()
            && let DataType::Struct(expected_fields) = &expect_partition_type
        {
            fields
                .iter()
                .zip_eq_fast(expected_fields.iter())
                .all(|(f1, f2)| f1.data_type() == f2.data_type())
        } else {
            false
        };
        if !is_same {
            return Err(icelake::Error::new(
                icelake::ErrorKind::ArrowError,
                format!(
                    "Expect partition type {:?}, but got {:?}",
                    expect_partition_type,
                    remove_field.data_type()
                ),
            ));
        }

        let arrow_partition_type_fields =
            if let DataType::Struct(fields) = self.partition_type.clone().try_into()? {
                fields
            } else {
                unreachable!()
            };
        let row_converter = RowConverter::new(vec![SortField::new(DataType::Struct(
            arrow_partition_type_fields.clone(),
        ))])
        .map_err(|e| icelake::Error::new(icelake::ErrorKind::ArrowError, e.to_string()))?;

        Ok(PrecomputedPartitionedWriter {
            inner_writers: HashMap::new(),
            inner_buidler: self.inner,
            schema: new_schema.into(),
            partition_type: self.partition_type,
            row_converter,
            partition_col_idx: self.partition_col_idx,
        })
    }
}

/// Partition append only writer
pub struct PrecomputedPartitionedWriter<B: IcebergWriterBuilder> {
    inner_writers: HashMap<OwnedRow, (B::R, icelake::types::StructValue)>,
    partition_type: Any,
    row_converter: RowConverter,
    inner_buidler: B,
    schema: SchemaRef,
    partition_col_idx: usize,
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for PrecomputedPartitionedWriter<B> {
    type R = <<B as IcebergWriterBuilder>::R as IcebergWriter>::R;

    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    async fn write(&mut self, mut batch: RecordBatch) -> icelake::Result<()> {
        // Remove the partition value from batch.
        let partition_value = batch.remove_column(self.partition_col_idx);
        let value_array = struct_to_anyvalue_array_with_type(
            partition_value
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap(),
            self.partition_type.clone(),
        )?;

        // Group the batch by row value.
        // # TODO
        // Maybe optimize the alloc and clone
        let rows = self
            .row_converter
            .convert_columns(&[partition_value.clone()])
            .map_err(|e| icelake::Error::new(icelake::ErrorKind::ArrowError, e.to_string()))?;
        let mut filters = HashMap::new();
        rows.into_iter().enumerate().for_each(|(row_id, row)| {
            filters
                .entry(row.owned())
                .or_insert_with(|| (vec![false; batch.num_rows()], row_id))
                .0[row_id] = true;
        });

        for (row, filter) in filters {
            let filter_array: BooleanArray = filter.0.into();
            let partial_batch = filter_record_batch(&batch, &filter_array)
                .expect("We should guarantee the filter array is valid");
            match self.inner_writers.entry(row) {
                Entry::Occupied(mut writer) => {
                    writer.get_mut().0.write(partial_batch).await?;
                }
                Entry::Vacant(vacant) => {
                    let value = value_array.get(filter.1).unwrap().as_ref().unwrap().clone();
                    if let AnyValue::Struct(value) = value {
                        let new_writer = self.inner_buidler.clone().build(&self.schema).await?;
                        vacant
                            .insert((new_writer, value))
                            .0
                            .write(partial_batch)
                            .await?;
                    } else {
                        unreachable!()
                    }
                }
            }
        }
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    async fn flush(&mut self) -> icelake::Result<Vec<Self::R>> {
        let mut res_vec = vec![];
        let inner_writers = std::mem::take(&mut self.inner_writers);
        for (_key, (mut writer, value)) in inner_writers {
            let mut res = writer.flush().await?;
            res.iter_mut().for_each(|res| {
                res.set_partition(Some(value.clone()));
            });
            res_vec.extend(res);
        }
        Ok(res_vec)
    }
}

// #[cfg(test)]
// mod test {
//     use icelake::types::{PartitionSpec, PartitionField};
//     use itertools::Itertools;

//     pub fn create_partition() -> PartitionSpec {
//         PartitionSpec {
//             spec_id: 1,
//             fields: vec![PartitionField {
//                 source_column_id: 1,
//                 partition_field_id: 1,
//                 transform: crate::types::Transform::Identity,
//                 name: "col1".to_string(),
//             }],
//         }
//     }

//     #[tokio::test]
//     async fn test_partition_writer() {
//         let schema = create_schema(2);
//         let arrow_schema = create_arrow_schema(2);
//         let partition_spec = create_partition();
//         let partition_type = Any::Struct(partition_spec.partition_type(&schema).unwrap().into());

//         let to_write = create_batch(
//             &arrow_schema,
//             vec![
//                 vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
//                 vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
//             ],
//         );

//         let builder = FanoutPartitionedWriterBuilder::new(
//             TestWriterBuilder {},
//             partition_type,
//             partition_spec,
//         );
//         let mut writer = builder.build(&arrow_schema).await.unwrap();
//         writer.write(to_write).await.unwrap();

//         assert_eq!(writer.inner_writers.len(), 3);

//         let expect1 = create_batch(&arrow_schema, vec![vec![1, 1, 1], vec![1, 2, 3]]);
//         let expect2 = create_batch(&arrow_schema, vec![vec![2, 2, 2], vec![1, 2, 3]]);
//         let expect3 = create_batch(&arrow_schema, vec![vec![3, 3, 3], vec![1, 2, 3]]);
//         let actual_res = writer
//             .inner_writers
//             .values()
//             .map(|writer| writer.res())
//             .collect_vec();
//         assert!(actual_res.contains(&expect1));
//         assert!(actual_res.contains(&expect2));
//         assert!(actual_res.contains(&expect3));
//     }

//     // # NOTE
//     // The delta writer will put the op vec in the last column, this test case test that the partition will
//     // ignore the last column.
//     #[tokio::test]
//     async fn test_partition_delta_writer() {
//         let schema = create_schema(2);
//         let arrow_schema = create_arrow_schema(3);
//         let partition_spec = create_partition();
//         let partition_type = Any::Struct(partition_spec.partition_type(&schema).unwrap().into());

//         let builder = FanoutPartitionedWriterBuilder::new(
//             TestWriterBuilder {},
//             partition_type,
//             partition_spec,
//         );
//         let mut writer = builder.build(&arrow_schema).await.unwrap();

//         let to_write = create_batch(
//             &arrow_schema,
//             vec![
//                 vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
//                 vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
//                 vec![3, 2, 1, 1, 3, 2, 2, 1, 3],
//             ],
//         );
//         writer.write(to_write).await.unwrap();
//         assert_eq!(writer.inner_writers.len(), 3);
//         let expect1 = create_batch(
//             &arrow_schema,
//             vec![vec![1, 1, 1], vec![1, 2, 3], vec![3, 1, 2]],
//         );
//         let expect2 = create_batch(
//             &arrow_schema,
//             vec![vec![2, 2, 2], vec![1, 2, 3], vec![2, 3, 1]],
//         );
//         let expect3 = create_batch(
//             &arrow_schema,
//             vec![vec![3, 3, 3], vec![1, 2, 3], vec![1, 2, 3]],
//         );
//         let actual_res = writer
//             .inner_writers
//             .values()
//             .map(|writer| writer.res())
//             .collect_vec();
//         assert!(actual_res.contains(&expect1));
//         assert!(actual_res.contains(&expect2));
//         assert!(actual_res.contains(&expect3));
//     }
// }
