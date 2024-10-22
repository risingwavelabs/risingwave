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
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::source::iceberg::{IcebergProperties, IcebergSplit};
use risingwave_connector::source::{ConnectorProperties, SplitImpl, SplitMetaData};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::error::BatchError;
use crate::executor::{DataChunk, Executor};
use crate::monitor::BatchMetrics;
use crate::task::BatchTaskContext;

static POSITION_DELETE_FILE_FILE_PATH_INDEX: usize = 0;
static POSITION_DELETE_FILE_POS: usize = 1;
pub struct IcebergScanExecutor {
    iceberg_config: IcebergProperties,
    #[allow(dead_code)]
    snapshot_id: Option<i64>,
    table_meta: TableMetadata,
    data_file_scan_tasks: Vec<FileScanTask>,
    equality_delete_file_scan_tasks: Vec<FileScanTask>,
    position_delete_file_scan_tasks: Vec<FileScanTask>,
    batch_size: usize,
    schema: Schema,
    identity: String,
    metrics: Option<BatchMetrics>,
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
        iceberg_config: IcebergProperties,
        snapshot_id: Option<i64>,
        table_meta: TableMetadata,
        data_file_scan_tasks: Vec<FileScanTask>,
        equality_delete_file_scan_tasks: Vec<FileScanTask>,
        position_delete_file_scan_tasks: Vec<FileScanTask>,
        batch_size: usize,
        schema: Schema,
        identity: String,
        metrics: Option<BatchMetrics>,
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
            metrics,
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
        let table_name = table.identifier().name().to_string();

        let data_file_scan_tasks = mem::take(&mut self.data_file_scan_tasks);

        let mut position_delete_filter = PositionDeleteFilter::new(
            mem::take(&mut self.position_delete_file_scan_tasks),
            &data_file_scan_tasks,
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

        // Delete rows in the data file that need to be deleted by map
        let mut read_bytes = 0;
        let _metrics_report_guard = scopeguard::guard(
            (read_bytes, table_name, self.metrics.clone()),
            |(read_bytes, table_name, metrics)| {
                if let Some(metrics) = metrics {
                    metrics
                        .iceberg_scan_metrics()
                        .iceberg_read_bytes
                        .with_guarded_label_values(&[&table_name])
                        .inc_by(read_bytes as _);
                }
            },
        );

        for data_file_scan_task in data_file_scan_tasks {
            let data_file_path = data_file_scan_task.data_file_path.clone();
            let data_sequence_number = data_file_scan_task.sequence_number;

            equality_delete_filter.apply_data_file_scan_task(&data_file_scan_task);

            let reader = table
                .reader_builder()
                .with_batch_size(self.batch_size)
                .build();
            let file_scan_stream = tokio_stream::once(Ok(data_file_scan_task));

            let mut record_batch_stream = reader.read(Box::pin(file_scan_stream))?.enumerate();

            while let Some((index, record_batch)) = record_batch_stream.next().await {
                let record_batch = record_batch?;

                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
                // position delete
                let chunk = position_delete_filter.filter(&data_file_path, chunk, index);
                // equality delete
                let chunk = equality_delete_filter.filter(chunk, data_sequence_number)?;
                assert_eq!(chunk.data_types(), data_types);
                read_bytes += chunk.estimated_heap_size() as u64;
                yield chunk;
            }
            position_delete_filter.remove_file_path(&data_file_path);
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
        let config = ConnectorProperties::extract(options_with_secret.clone(), false)?;

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
        let metrics = source.context.batch_metrics().clone();

        if let ConnectorProperties::Iceberg(iceberg_properties) = config
            && let SplitImpl::Iceberg(split) = &split_list[0]
        {
            let iceberg_properties: IcebergProperties = *iceberg_properties;
            let split: IcebergSplit = split.clone();
            Ok(Box::new(IcebergScanExecutor::new(
                iceberg_properties,
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
                metrics,
            )))
        } else {
            unreachable!()
        }
    }
}

struct PositionDeleteFilter {
    // Delete columns pos for each file path, false means this column needs to be deleted, value is divided by batch size
    position_delete_file_path_pos_map: HashMap<String, HashMap<usize, Vec<bool>>>,
}

impl PositionDeleteFilter {
    async fn new(
        position_delete_file_scan_tasks: Vec<FileScanTask>,
        data_file_scan_tasks: &[FileScanTask],
        table: &Table,
        batch_size: usize,
    ) -> crate::error::Result<Self> {
        let mut position_delete_file_path_pos_map: HashMap<String, HashMap<usize, Vec<bool>>> =
            HashMap::default();
        let data_file_path_set = data_file_scan_tasks
            .iter()
            .map(|data_file_scan_task| data_file_scan_task.data_file_path.as_ref())
            .collect::<std::collections::HashSet<_>>();

        let position_delete_file_scan_stream = {
            #[try_stream]
            async move {
                for position_delete_file_scan_task in position_delete_file_scan_tasks {
                    yield position_delete_file_scan_task;
                }
            }
        };

        let reader = table.reader_builder().with_batch_size(batch_size).build();

        let mut record_batch_stream = reader.read(Box::pin(position_delete_file_scan_stream))?;

        while let Some(record_batch) = record_batch_stream.next().await {
            let record_batch = record_batch?;
            let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
            for row in chunk.rows() {
                // The schema is fixed. `0` must be `file_path`, `1` must be `pos`.
                if let Some(ScalarRefImpl::Utf8(file_path)) =
                    row.datum_at(POSITION_DELETE_FILE_FILE_PATH_INDEX)
                    && let Some(ScalarRefImpl::Int64(pos)) = row.datum_at(POSITION_DELETE_FILE_POS)
                {
                    if !data_file_path_set.contains(file_path) {
                        continue;
                    }
                    let entry = position_delete_file_path_pos_map
                        .entry(file_path.to_string())
                        .or_default();
                    // Split `pos` by `batch_size`, because the data file will also be split by `batch_size`
                    let delete_vec_index = pos as usize / batch_size;
                    let delete_vec_pos = pos as usize % batch_size;
                    let delete_vec = entry
                        .entry(delete_vec_index)
                        .or_insert(vec![true; batch_size]);
                    delete_vec[delete_vec_pos] = false;
                } else {
                    bail!("position delete `file_path` and `pos` must be string and int64")
                }
            }
        }
        Ok(Self {
            position_delete_file_path_pos_map,
        })
    }

    fn filter(&self, data_file_path: &str, mut chunk: DataChunk, index: usize) -> DataChunk {
        chunk = chunk.compact();
        if let Some(position_delete_bool_iter) = self
            .position_delete_file_path_pos_map
            .get(data_file_path)
            .and_then(|delete_vecs| delete_vecs.get(&index))
        {
            // Some chunks are less than `batch_size`, so we need to be truncate to ensure that the bitmap length is consistent
            let position_delete_bool_iter = if position_delete_bool_iter.len() > chunk.capacity() {
                &position_delete_bool_iter[..chunk.capacity()]
            } else {
                position_delete_bool_iter
            };
            let new_visibility = Bitmap::from_bool_slice(position_delete_bool_iter);
            chunk.set_visibility(chunk.visibility().bitand(new_visibility));
            chunk
        } else {
            chunk
        }
    }

    fn remove_file_path(&mut self, file_path: &str) {
        self.position_delete_file_path_pos_map.remove(file_path);
    }
}

struct EqualityDeleteFilter {
    // The `seq_num` corresponding to each row in the equality delete file
    equality_delete_rows_seq_num_map: HashMap<OwnedRow, i64>,
    // The field ids of the equality delete columns
    equality_delete_ids: Option<Vec<i32>>,
    // In chunk, the indexes of the equality delete columns
    equality_delete_column_idxes: Option<Vec<usize>>,
    // The schema of the data file, which is the intersection of the output shema and the equality delete columns
    data_chunk_column_names: Option<Vec<String>>,
    // Column names for the output schema so that columns can be trimmed after filter
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

            let mut delete_record_batch_stream = reader.read(Box::pin(delete_file_scan_stream))?;

            while let Some(record_batch) = delete_record_batch_stream.next().await {
                let record_batch = record_batch?;

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
        if let Some(equality_delete_ids) = &self.equality_delete_ids {
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
            self.equality_delete_column_idxes = Some(
                equality_delete_ids
                    .iter()
                    .map(|equality_delete_id| {
                        data_file_scan_task
                            .project_field_ids
                            .iter()
                            .position(|project_field_id| equality_delete_id == project_field_id)
                            .expect("equality_delete_id not found in delete_equality_ids")
                    })
                    .collect_vec(),
            );
        }
    }

    fn filter(
        &self,
        mut chunk: DataChunk,
        data_sequence_number: i64,
    ) -> crate::error::Result<DataChunk> {
        chunk = chunk.compact();
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
