// Copyright 2024 RisingWave Labs
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

use core::ops::BitAnd;
use std::collections::HashMap;
use std::mem;

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::source::iceberg::{IcebergProperties, IcebergSplit};
use risingwave_connector::source::{ConnectorProperties, SplitImpl, SplitMetaData};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::error::BatchError;
use crate::executor::{DataChunk, Executor};
use crate::task::BatchTaskContext;

pub struct IcebergScanExecutor {
    iceberg_config: IcebergConfig,
    #[allow(dead_code)]
    snapshot_id: Option<i64>,
    table_meta: TableMetadata,
    data_file_scan_tasks: Vec<FileScanTask>,
    equality_delete_file_scan_tasks: Vec<FileScanTask>,
    position_delete_file_scan_tasks: Vec<FileScanTask>,
    batch_size: usize,
    schema: Schema,
    identity: String,
}

impl Executor for IcebergScanExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl IcebergScanExecutor {
    pub fn new(
        iceberg_config: IcebergConfig,
        snapshot_id: Option<i64>,
        table_meta: TableMetadata,
        data_file_scan_tasks: Vec<FileScanTask>,
        equality_delete_file_scan_tasks: Vec<FileScanTask>,
        position_delete_file_scan_tasks: Vec<FileScanTask>,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            table_meta,
            data_file_scan_tasks,
            equality_delete_file_scan_tasks,
            position_delete_file_scan_tasks,
            batch_size,
            schema,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let table = self
            .iceberg_config
            .load_table_v2_with_metadata(self.table_meta)
            .await?;
        let data_types = self.schema.data_types();
        let executor_schema_names = self.schema.names();

        let mut position_delete_filter = PositionDeleteFilter::new(
            mem::take(&mut self.position_delete_file_scan_tasks),
            &table,
            self.batch_size,
        )
        .await?;
        let mut equality_delete_filter = EqualityDeleteFilter::new(
            mem::take(&mut self.equality_delete_file_scan_tasks),
            &table,
            self.batch_size,
            executor_schema_names,
        )
        .await?;

        let data_file_scan_tasks = mem::take(&mut self.data_file_scan_tasks);

        // Delete rows in the data file that need to be deleted by map
        for data_file_scan_task in data_file_scan_tasks {
            let data_file_path = data_file_scan_task.data_file_path.clone();
            let data_sequence_number = data_file_scan_task.sequence_number;

            equality_delete_filter.apply_data_file_scan_task(&data_file_scan_task);

            let reader = table
                .reader_builder()
                .with_batch_size(self.batch_size)
                .build();
            let file_scan_stream = tokio_stream::once(Ok(data_file_scan_task));

            let mut record_batch_stream = reader
                .read(Box::pin(file_scan_stream))
                .map_err(BatchError::Iceberg)?
                .enumerate();

            while let Some((index, record_batch)) = record_batch_stream.next().await {
                let record_batch = record_batch.map_err(BatchError::Iceberg)?;

                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
                // position delete
                let chunk = position_delete_filter.filter(&data_file_path, chunk, index);
                // equality delete
                let chunk = equality_delete_filter.filter(chunk, data_sequence_number)?;
                assert_eq!(chunk.data_types(), data_types);
                yield chunk;
            }
        }
    }
}

pub struct IcebergScanExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for IcebergScanExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Iceberg source should not have input executor!"
        );
        let source_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::IcebergScan
        )?;

        // prepare connector source
        let options_with_secret = WithOptionsSecResolved::new(
            source_node.with_properties.clone(),
            source_node.secret_refs.clone(),
        );
        let config = ConnectorProperties::extract(options_with_secret.clone(), false)
            .map_err(BatchError::connector)?;

        let split_list = source_node
            .split
            .iter()
            .map(|split| SplitImpl::restore_from_bytes(split).unwrap())
            .collect_vec();
        assert_eq!(split_list.len(), 1);

        let fields = source_node
            .columns
            .iter()
            .map(|prost| {
                let column_desc = prost.column_desc.as_ref().unwrap();
                let data_type = DataType::from(column_desc.column_type.as_ref().unwrap());
                let name = column_desc.name.clone();
                Field::with_name(data_type, name)
            })
            .collect();
        let schema = Schema::new(fields);

        if let ConnectorProperties::Iceberg(iceberg_properties) = config
            && let SplitImpl::Iceberg(split) = &split_list[0]
        {
            let iceberg_properties: IcebergProperties = *iceberg_properties;
            let split: IcebergSplit = split.clone();
            Ok(Box::new(IcebergScanExecutor::new(
                iceberg_properties.to_iceberg_config(),
                Some(split.snapshot_id),
                split.table_meta.deserialize(),
                split.files.into_iter().map(|x| x.deserialize()).collect(),
                split
                    .equality_delete_files
                    .into_iter()
                    .map(|x| x.deserialize())
                    .collect(),
                split
                    .position_delete_files
                    .into_iter()
                    .map(|x| x.deserialize())
                    .collect(),
                source.context.get_config().developer.chunk_size,
                schema,
                source.plan_node().get_identity().clone(),
            )))
        } else {
            unreachable!()
        }
    }
}

struct PositionDeleteFilter {
    position_delete_file_path_pos_map: HashMap<String, Vec<Vec<bool>>>,
}

impl PositionDeleteFilter {
    async fn new(
        position_delete_file_scan_tasks: Vec<FileScanTask>,
        table: &Table,
        batch_size: usize,
    ) -> crate::error::Result<Self> {
        let mut position_delete_file_path_pos_map: HashMap<String, Vec<Vec<bool>>> =
            HashMap::default();

        let position_delete_file_scan_stream = {
            #[try_stream]
            async move {
                for position_delete_file_scan_task in position_delete_file_scan_tasks {
                    yield position_delete_file_scan_task;
                }
            }
        };

        let reader = table.reader_builder().with_batch_size(batch_size).build();

        let mut record_batch_stream = reader
            .read(Box::pin(position_delete_file_scan_stream))
            .map_err(BatchError::Iceberg)?;

        while let Some(record_batch) = record_batch_stream.next().await {
            let record_batch = record_batch.map_err(BatchError::Iceberg)?;
            let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
            for row in chunk.rows() {
                if let Some(file_path) = row.datum_at(0)
                    && let Some(pos) = row.datum_at(1)
                {
                    if let ScalarRefImpl::Utf8(file_path) = file_path
                        && let ScalarRefImpl::Int64(pos) = pos
                    {
                        let entry = position_delete_file_path_pos_map
                            .entry(file_path.to_string())
                            .or_default();
                        let delete_vec_index = pos as usize / batch_size;
                        let delete_vec_pos = pos as usize % batch_size;
                        if delete_vec_index >= entry.len() {
                            entry.resize(delete_vec_index + 1, vec![true; batch_size]);
                        }
                        entry[delete_vec_index][delete_vec_pos] = false;
                    } else {
                        bail!("position delete `file_path` and `pos` must be string and int64")
                    }
                }
            }
        }
        Ok(Self {
            position_delete_file_path_pos_map,
        })
    }

    fn filter(&mut self, data_file_path: &str, mut chunk: DataChunk, index: usize) -> DataChunk {
        if !chunk.is_compacted(){
            chunk = chunk.compact();
        }
        let mut position_delete_bool_iter = self
            .position_delete_file_path_pos_map
            .get(data_file_path)
            .map(|delete_vecs| delete_vecs.get(index).cloned())
            .flatten()
            .unwrap_or_else(|| vec![true; chunk.capacity()]);
        position_delete_bool_iter.truncate(chunk.capacity());
        let new_visibility = Bitmap::from_iter(position_delete_bool_iter);
        chunk.set_visibility(chunk.visibility().bitand(new_visibility));
        chunk
    }
}

struct EqualityDeleteFilter {
    equality_delete_rows_seq_num_map: HashMap<OwnedRow, i64>,
    equality_delete_ids: Option<Vec<i32>>,
    equality_delete_column_idxes: Option<Vec<usize>>,
    data_chunk_column_names: Option<Vec<String>>,
    executor_schema_names: Vec<String>,
}

impl EqualityDeleteFilter {
    async fn new(
        equality_delete_file_scan_tasks: Vec<FileScanTask>,
        table: &Table,
        batch_size: usize,
        executor_schema_names: Vec<String>,
    ) -> crate::error::Result<Self> {
        let mut equality_delete_rows_seq_num_map: HashMap<OwnedRow, i64> = HashMap::default();

        // Build hash map for equality delete files
        // Currently, all equality delete files have the same schema which is guaranteed by `IcebergSplitEnumerator`.
        let mut equality_delete_ids: Option<Vec<_>> = None;
        for equality_delete_file_scan_task in equality_delete_file_scan_tasks {
            let mut sequence_number = equality_delete_file_scan_task.sequence_number;

            if equality_delete_ids.is_none() {
                equality_delete_ids =
                    Some(equality_delete_file_scan_task.project_field_ids.clone());
            } else {
                debug_assert_eq!(
                    equality_delete_ids.as_ref().unwrap(),
                    &equality_delete_file_scan_task.project_field_ids
                );
            }

            let reader = table.reader_builder().with_batch_size(batch_size).build();
            let delete_file_scan_stream = tokio_stream::once(Ok(equality_delete_file_scan_task));

            let mut delete_record_batch_stream = reader
                .read(Box::pin(delete_file_scan_stream))
                .map_err(BatchError::Iceberg)?;

            while let Some(record_batch) = delete_record_batch_stream.next().await {
                let record_batch = record_batch.map_err(BatchError::Iceberg)?;

                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
                for row in chunk.rows() {
                    let entry = equality_delete_rows_seq_num_map
                        .entry(row.to_owned_row())
                        .or_default();
                    *entry = *entry.max(&mut sequence_number);
                }
            }
        }
        Ok(Self {
            equality_delete_rows_seq_num_map,
            equality_delete_ids,
            equality_delete_column_idxes: None,
            data_chunk_column_names: None,
            executor_schema_names,
        })
    }

    fn apply_data_file_scan_task(&mut self, data_file_scan_task: &FileScanTask) {
        self.data_chunk_column_names = Some(
            data_file_scan_task
                .project_field_ids
                .iter()
                .filter_map(|id| {
                    data_file_scan_task
                        .schema
                        .name_by_field_id(*id)
                        .map(|name| name.to_string())
                })
                .collect(),
        );
        // eq_delete_column_idxes are used to fetch equality delete columns from data files.
        self.equality_delete_column_idxes =
            self.equality_delete_ids
                .as_ref()
                .map(|equality_delete_ids| {
                    equality_delete_ids
                        .iter()
                        .map(|equality_delete_id| {
                            data_file_scan_task
                                .project_field_ids
                                .iter()
                                .position(|project_field_id| equality_delete_id == project_field_id)
                                .expect("equality_delete_id not found in delete_equality_ids")
                        })
                        .collect_vec()
                });
    }

    fn filter(
        &self,
        mut chunk: DataChunk,
        data_sequence_number: i64,
    ) -> crate::error::Result<DataChunk> {
        if !chunk.is_compacted(){
            chunk = chunk.compact();
        }
        match self.equality_delete_column_idxes.as_ref() {
            Some(delete_column_ids) => {
                let new_visibility = Bitmap::from_iter(
                    // Project with the schema of the delete file
                    chunk.project(delete_column_ids).rows().map(|row_ref| {
                        let row = row_ref.to_owned_row();
                        if let Some(delete_sequence_number) =
                            self.equality_delete_rows_seq_num_map.get(&row)
                            && delete_sequence_number > &data_sequence_number
                        {
                            // delete_sequence_number > data_sequence_number means the delete file is written later than data file,
                            // so it needs to be deleted
                            false
                        } else {
                            true
                        }
                    }),
                )
                .clone();
                let Some(ref data_chunk_column_names) = self.data_chunk_column_names else {
                    bail!("data_chunk_column_names is not set")
                };

                // Keep the schema consistent(chunk and executor)
                // Filter out (equality delete) columns that are not in the executor schema
                let (data, old_visibility) = chunk.into_parts_v2();
                let data = data
                    .iter()
                    .zip_eq_fast(data_chunk_column_names)
                    .filter_map(|(array, columns)| {
                        if self.executor_schema_names.contains(columns) {
                            Some(array.clone())
                        } else {
                            None
                        }
                    })
                    .collect_vec();
                let chunk = DataChunk::new(data, old_visibility.bitand(new_visibility));
                Ok(chunk)
            }
            // If there is no delete file, the data file is directly output
            None => Ok(chunk),
        }
    }
}
