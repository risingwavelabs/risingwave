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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use datafusion_common::{DataFusionError, internal_err, not_impl_err};
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use iceberg::scan::FileScanTask;
use iceberg::spec::DataContentType;
use iceberg::table::Table;
use risingwave_common::catalog::{
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::{IcebergTableProvider, to_datafusion_error};

/// An execution plan for scanning Iceberg tables.
///
/// It utilizes the `IcebergProperties` to read data and produces `RecordBatches` accordingly.
#[derive(Debug, Clone)]
pub struct IcebergScan {
    inner: Arc<IcebergScanInner>,
}

#[derive(Debug)]
struct IcebergScanInner {
    table: Table,
    snapshot_id: Option<i64>,
    tasks: Vec<FileScanTask>,
    #[allow(dead_code)]
    iceberg_scan_type: IcebergScanType,
    arrow_schema: SchemaRef,
    column_names: Vec<String>,
    #[allow(dead_code)]
    need_seq_num: bool,
    #[allow(dead_code)]
    need_file_path_and_pos: bool,
    plan_properties: PlanProperties,
}

impl DisplayAs for IcebergScan {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        // TODO: improve the display format
        write!(f, "{:?}", self)
    }
}

impl ExecutionPlan for IcebergScan {
    fn name(&self) -> &str {
        "IcebergScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.inner.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if partition >= self.inner.tasks.len() {
            return Err(DataFusionError::Internal(
                "IcebergScan: partition out of bounds".to_owned(),
            ));
        }

        let chunk_size = context.session_config().batch_size();
        let stream = self.inner.clone().execute_inner(chunk_size, partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl IcebergScan {
    pub async fn new(
        provider: &IcebergTableProvider,
        // TODO: handle these params
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Self> {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(provider.schema()),
            // TODO: determine partitioning
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let mut column_names: Vec<String> = provider
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let table = provider
            .iceberg_properties
            .load_table()
            .await
            .map_err(to_datafusion_error)?;
        let need_seq_num = column_names
            .iter()
            .any(|name| name == ICEBERG_SEQUENCE_NUM_COLUMN_NAME);
        let need_file_path_and_pos = column_names
            .iter()
            .any(|name| name == ICEBERG_FILE_PATH_COLUMN_NAME)
            && matches!(provider.iceberg_scan_type, IcebergScanType::DataScan);
        column_names.retain(|name| {
            ![
                ICEBERG_FILE_PATH_COLUMN_NAME,
                ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
                ICEBERG_FILE_POS_COLUMN_NAME,
            ]
            .contains(&name.as_str())
        });

        let mut inner = IcebergScanInner {
            table,
            snapshot_id: provider.snapshot_id,
            tasks: vec![],
            iceberg_scan_type: provider.iceberg_scan_type,
            arrow_schema: provider.arrow_schema.clone(),
            column_names,
            need_seq_num,
            need_file_path_and_pos,
            plan_properties,
        };
        inner.tasks = inner.list_iceberg_scan_task().try_collect().await?;
        inner.plan_properties.partitioning = Partitioning::UnknownPartitioning(inner.tasks.len());

        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

impl IcebergScanInner {
    #[try_stream(ok = FileScanTask, error = DataFusionError)]
    async fn list_iceberg_scan_task(&self) {
        let mut scan_builder = self.table.scan().select(&self.column_names);
        if let Some(snapshot_id) = self.snapshot_id {
            scan_builder = scan_builder.snapshot_id(snapshot_id);
        }
        if matches!(
            self.iceberg_scan_type,
            IcebergScanType::EqualityDeleteScan | IcebergScanType::PositionDeleteScan
        ) {
            scan_builder = scan_builder.with_delete_file_processing_enabled(true);
        }
        let scan = scan_builder.build().map_err(to_datafusion_error)?;

        let mut position_delete_files_set = HashSet::new();
        let mut equality_delete_files_set = HashSet::new();

        #[for_await]
        for scan_task in scan.plan_files().await.map_err(to_datafusion_error)? {
            let scan_task = scan_task.map_err(to_datafusion_error)?;
            match self.iceberg_scan_type {
                IcebergScanType::DataScan => {
                    if scan_task.data_file_content != DataContentType::Data {
                        return internal_err!(
                            "Files of type {:?} should not be in the data files",
                            scan_task.data_file_content
                        );
                    }
                    yield scan_task;
                }
                IcebergScanType::EqualityDeleteScan => {
                    for delete_file in scan_task.deletes {
                        if delete_file.data_file_content == DataContentType::EqualityDeletes
                            && equality_delete_files_set.insert(delete_file.data_file_path.clone())
                        {
                            yield delete_file.as_ref().clone()
                        }
                    }
                }
                IcebergScanType::PositionDeleteScan => {
                    for delete_file in scan_task.deletes {
                        if delete_file.data_file_content == DataContentType::PositionDeletes
                            && position_delete_files_set.insert(delete_file.data_file_path.clone())
                        {
                            let mut task = delete_file.as_ref().clone();
                            task.project_field_ids = Vec::new();
                            yield task;
                        }
                    }
                }
                _ => {
                    return not_impl_err!(
                        "Iceberg scan type {:?} is not supported",
                        self.iceberg_scan_type
                    );
                }
            }
        }
    }

    #[try_stream(ok = RecordBatch, error = DataFusionError)]
    pub async fn execute_inner(self: Arc<Self>, chunk_size: usize, partition: usize) {
        let mut buffer = RecordBatchBuffer::new(chunk_size);
        let reader = self
            .table
            .reader_builder()
            .with_batch_size(chunk_size)
            .build();
        let task = self.tasks[partition].clone();
        let stream = reader
            .read(tokio_stream::once(Ok(task)).boxed())
            .await
            .map_err(to_datafusion_error)?;

        #[for_await]
        for (i, batch) in stream.enumerate() {
            let batch = batch.map_err(to_datafusion_error)?;
            let batch = append_metadata(
                batch,
                self.need_seq_num,
                self.need_file_path_and_pos,
                &self.tasks[partition],
                (i * chunk_size).try_into().unwrap(),
            )?;
            let batch = cast_batch(self.arrow_schema.clone(), batch)?;
            if let Some(batch) = buffer.add(batch)? {
                yield batch;
            }
        }

        if let Some(batch) = buffer.finish()? {
            yield batch;
        }
    }
}

struct RecordBatchBuffer {
    buffer: Vec<RecordBatch>,
    current_rows: usize,
    max_record_batch_rows: usize,
}

impl RecordBatchBuffer {
    fn new(max_record_batch_rows: usize) -> Self {
        Self {
            buffer: vec![],
            current_rows: 0,
            max_record_batch_rows,
        }
    }

    fn add(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>, DataFusionError> {
        // Case 1: New batch itself is large enough and buffer is empty or too small to be significant
        if batch.num_rows() >= self.max_record_batch_rows && self.buffer.is_empty() {
            // Buffer was empty, yield current large batch directly
            return Ok(Some(batch));
        }

        // Case 2: Buffer will overflow with the new batch
        if !self.buffer.is_empty()
            && (self.current_rows + batch.num_rows() > self.max_record_batch_rows)
        {
            let combined = self.finish_internal()?; // Drain and combine buffer
            self.current_rows = batch.num_rows();
            self.buffer.push(batch); // Add current batch to now-empty buffer
            return Ok(combined); // Return the combined batch from buffer
        }

        // Case 3: Buffer has space
        self.current_rows += batch.num_rows();
        self.buffer.push(batch);
        Ok(None)
    }

    // Helper to drain and combine buffer, used by add and finish
    fn finish_internal(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let schema_to_use = self.buffer[0].schema();
        let batches_to_combine = std::mem::take(&mut self.buffer);
        let combined = concat_batches(&schema_to_use, &batches_to_combine)?;
        self.current_rows = 0;
        Ok(Some(combined))
    }

    fn finish(mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        self.finish_internal()
    }
}

fn append_metadata(
    batch: RecordBatch,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
    task: &FileScanTask,
    pos_start: i64,
) -> Result<RecordBatch, DataFusionError> {
    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();

    if need_seq_num {
        let seq_num_array = Int64Array::from_value(task.sequence_number, batch.num_rows());
        columns.push(Arc::new(seq_num_array));
        fields.push(Arc::new(Field::new(
            ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
            DataType::Int64,
            false,
        )));
    }

    if need_file_path_and_pos {
        let file_path_array = StringArray::from_iter_values(std::iter::repeat_n(
            task.data_file_path(),
            batch.num_rows(),
        ));
        let file_pos_array = Int64Array::from_iter((pos_start..).take(batch.num_rows()));
        columns.push(Arc::new(file_path_array));
        columns.push(Arc::new(file_pos_array));
        fields.push(Arc::new(Field::new(
            ICEBERG_FILE_PATH_COLUMN_NAME,
            DataType::Utf8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            ICEBERG_FILE_POS_COLUMN_NAME,
            DataType::Int64,
            false,
        )));
    }

    let new_schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(new_schema, columns).map_err(to_datafusion_error)
}

fn cast_batch(
    target_schema: SchemaRef,
    batch: RecordBatch,
) -> Result<RecordBatch, DataFusionError> {
    if batch.num_columns() != target_schema.fields().len() {
        return Err(DataFusionError::Internal(
            "column count must match schema column count".to_owned(),
        ));
    }

    let mut target_columns = Vec::with_capacity(batch.num_columns());
    for (column, target_field) in batch.columns().iter().zip_eq_fast(target_schema.fields()) {
        if column.data_type() == target_field.data_type() {
            target_columns.push(column.clone());
        } else {
            let casted_column = datafusion::arrow::compute::cast(column, target_field.data_type())?;
            target_columns.push(casted_column);
        }
    }

    let res = RecordBatch::try_new(target_schema.clone(), target_columns)?;
    Ok(res)
}
