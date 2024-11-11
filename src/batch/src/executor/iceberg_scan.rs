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
use std::sync::Arc;

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayImpl, I64Array};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{Field, Schema, ICEBERG_SEQUENCE_NUM_COLUMN_NAME};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::source::iceberg::{
    IcebergFileScanTaskJsonStrEnum, IcebergProperties, IcebergSplit,
};
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

pub enum IcebergFileScanTaskEnum {
    // The scan task of the data file and the position delete file
    DataAndPositionDelete(Vec<FileScanTask>, Vec<FileScanTask>),
    // The scan task of the equality delete file
    EqualityDelete(Vec<FileScanTask>),
}

impl IcebergFileScanTaskEnum {
    fn from_iceberg_file_scan_task_json_str_enum(
        iceberg_file_scan_task_json_str_enum: IcebergFileScanTaskJsonStrEnum,
    ) -> Self {
        match iceberg_file_scan_task_json_str_enum {
            IcebergFileScanTaskJsonStrEnum::DataAndPositionDelete(
                data_file_scan_tasks,
                position_delete_file_scan_tasks,
            ) => IcebergFileScanTaskEnum::DataAndPositionDelete(
                data_file_scan_tasks
                    .into_iter()
                    .map(|t| t.deserialize())
                    .collect(),
                position_delete_file_scan_tasks
                    .into_iter()
                    .map(|t| t.deserialize())
                    .collect(),
            ),
            IcebergFileScanTaskJsonStrEnum::EqualityDelete(equality_delete_file_scan_tasks) => {
                IcebergFileScanTaskEnum::EqualityDelete(
                    equality_delete_file_scan_tasks
                        .into_iter()
                        .map(|t| t.deserialize())
                        .collect(),
                )
            }
        }
    }
}

pub struct IcebergScanExecutor {
    iceberg_config: IcebergProperties,
    #[allow(dead_code)]
    snapshot_id: Option<i64>,
    table_meta: TableMetadata,
    file_scan_tasks: Option<IcebergFileScanTaskEnum>,
    batch_size: usize,
    schema: Schema,
    identity: String,
    metrics: Option<BatchMetrics>,
    need_seq_num: bool,
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
        file_scan_tasks: IcebergFileScanTaskEnum,
        batch_size: usize,
        schema: Schema,
        identity: String,
        metrics: Option<BatchMetrics>,
        need_seq_num: bool,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            table_meta,
            batch_size,
            schema,
            file_scan_tasks: Some(file_scan_tasks),
            identity,
            metrics,
            need_seq_num,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let table = self
            .iceberg_config
            .load_table_v2_with_metadata(self.table_meta)
            .await?;
        let data_types = self.schema.data_types();
        let table_name = table.identifier().name().to_string();

        let (mut position_delete_filter, data_file_scan_tasks) =
            match Option::take(&mut self.file_scan_tasks) {
                Some(IcebergFileScanTaskEnum::DataAndPositionDelete(
                    data_file_scan_tasks,
                    position_delete_file_scan_tasks,
                )) => (
                    Some(
                        PositionDeleteFilter::new(
                            position_delete_file_scan_tasks,
                            &data_file_scan_tasks,
                            &table,
                            self.batch_size,
                        )
                        .await?,
                    ),
                    data_file_scan_tasks,
                ),
                Some(IcebergFileScanTaskEnum::EqualityDelete(equality_delete_file_scan_tasks)) => {
                    (None, equality_delete_file_scan_tasks)
                }
                None => {
                    bail!("file_scan_tasks must be Some")
                }
            };

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

            let reader = table
                .reader_builder()
                .with_batch_size(self.batch_size)
                .build();
            let file_scan_stream = tokio_stream::once(Ok(data_file_scan_task));

            let mut record_batch_stream = reader.read(Box::pin(file_scan_stream))?.enumerate();

            while let Some((index, record_batch)) = record_batch_stream.next().await {
                let record_batch = record_batch?;

                // iceberg_t1_source
                let mut chunk = if self.need_seq_num {
                    let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
                    let (mut columns, visibility) = chunk.into_parts();
                    columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(
                        vec![data_sequence_number; visibility.len()],
                    ))));
                    DataChunk::from_parts(columns.into(), visibility)
                } else {
                    IcebergArrowConvert.chunk_from_record_batch(&record_batch)?
                };

                // position delete
                if let Some(position_delete_filter) = &mut position_delete_filter {
                    chunk = position_delete_filter.filter(&data_file_path, chunk, index);
                }
                assert_eq!(chunk.data_types(), data_types);
                read_bytes += chunk.estimated_heap_size() as u64;
                yield chunk;
            }
            if let Some(position_delete_filter) = &mut position_delete_filter {
                position_delete_filter.remove_file_path(&data_file_path);
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
            let need_seq_num = schema
                .fields()
                .iter()
                .any(|f| f.name == ICEBERG_SEQUENCE_NUM_COLUMN_NAME);
            Ok(Box::new(IcebergScanExecutor::new(
                iceberg_properties,
                Some(split.snapshot_id),
                split.table_meta.deserialize(),
                IcebergFileScanTaskEnum::from_iceberg_file_scan_task_json_str_enum(split.files),
                source.context.get_config().developer.chunk_size,
                schema,
                source.plan_node().get_identity().clone(),
                metrics,
                need_seq_num,
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
