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
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use datafusion_common::DataFusionError;
use futures_async_stream::try_stream;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::iceberg::IcebergProperties;
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
    iceberg_properties: Arc<IcebergProperties>,
    snapshot_id: Option<i64>,
    #[allow(dead_code)]
    iceberg_scan_type: IcebergScanType,
    arrow_schema: SchemaRef,
    column_names: Option<Vec<String>>,
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
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "IcebergScan only supports single partition".to_owned(),
            ));
        }

        let chunk_size = context.session_config().batch_size();
        let stream = self.inner.clone().execute_inner(chunk_size);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl IcebergScan {
    pub fn new(
        provider: &IcebergTableProvider,
        // TODO: handle these params
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Self> {
        if provider.iceberg_scan_type != IcebergScanType::DataScan {
            return Err(DataFusionError::NotImplemented(
                "Only DataScan is supported currently".to_owned(),
            ));
        }

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(provider.schema()),
            // TODO: determine partitioning
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let column_names = provider
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let inner = IcebergScanInner {
            iceberg_properties: provider.iceberg_properties.clone(),
            snapshot_id: provider.snapshot_id,
            iceberg_scan_type: provider.iceberg_scan_type,
            arrow_schema: provider.arrow_schema.clone(),
            column_names: Some(column_names),
            need_seq_num: false,
            need_file_path_and_pos: false,
            plan_properties,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

impl IcebergScanInner {
    #[try_stream(ok = RecordBatch, error = DataFusionError)]
    pub async fn execute_inner(self: Arc<Self>, chunk_size: usize) {
        let mut buffer = RecordBatchBuffer::new(chunk_size);
        let table = self
            .iceberg_properties
            .load_table()
            .await
            .map_err(to_datafusion_error)?;
        let mut scan_builder = table.scan().with_batch_size(Some(chunk_size));
        if let Some(column_names) = &self.column_names {
            scan_builder = scan_builder.select(column_names);
        }
        if let Some(snapshot_id) = self.snapshot_id {
            scan_builder = scan_builder.snapshot_id(snapshot_id);
        }
        let scan = scan_builder.build().map_err(to_datafusion_error)?;
        let stream = scan.to_arrow().await.map_err(to_datafusion_error)?;

        #[for_await]
        for batch in stream {
            let batch = batch.map_err(to_datafusion_error)?;
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
        let batches_to_combine: Vec<_> = std::mem::take(&mut self.buffer);
        let combined = concat_batches(&schema_to_use, &batches_to_combine)?;
        self.current_rows = 0;
        Ok(Some(combined))
    }

    fn finish(mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        self.finish_internal()
    }
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
