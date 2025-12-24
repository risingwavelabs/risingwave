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

use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig as DFSessionConfig, SessionContext as DFSessionContext};
use datafusion_common::DFSchema;
use datafusion_common::arrow::datatypes::DataType as DFDataType;
use futures_async_stream::for_await;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Format;
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::Schema as RwSchema;
use risingwave_common::error::BoxedError;
use risingwave_common::types::DataType as RwDataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use tokio::sync::mpsc;

use crate::PgResponseStream;
use crate::error::{ErrorCode, Result as RwResult};
use crate::expr::{Expr, ExprImpl, InputRef};
use crate::handler::RwPgResponse;
use crate::handler::util::{DataChunkToRowSetAdapter, to_pg_field};
use crate::scheduler::SchedulerError;
use crate::session::SessionImpl;

pub struct DfBatchQueryPlanResult {
    pub(crate) plan: Arc<datafusion::logical_expr::LogicalPlan>,
    pub(crate) schema: RwSchema,
    pub(crate) stmt_type: StatementType,
}

pub async fn execute_datafusion_plan(
    session: Arc<SessionImpl>,
    plan: DfBatchQueryPlanResult,
    formats: Vec<Format>,
) -> RwResult<RwPgResponse> {
    let df_config = create_config(session.as_ref());
    let ctx = DFSessionContext::new_with_config(df_config);
    let state = ctx.state();

    let pg_descs: Vec<PgFieldDescriptor> = plan.schema.fields().iter().map(to_pg_field).collect();
    let column_types = plan.schema.fields().iter().map(|f| f.data_type()).collect();

    // TODO: some optimizing rules will cause inconsistency, need to investigate later
    // Currently we disable all optimizing rules to ensure correctness
    let df_plan = state.analyzer().execute_and_check(
        plan.plan.as_ref().clone(),
        &ConfigOptions::default(),
        |_, _| {},
    )?;
    let physical_plan = state
        .query_planner()
        .create_physical_plan(&df_plan, &state)
        .await?;
    let data_stream = execute_stream(physical_plan, ctx.task_ctx())?;

    let compute_runtime = session.env().compute_runtime();
    let (sender1, receiver) = mpsc::channel(10);
    let shutdown_rx = session.reset_cancel_query_flag();
    let sender2 = sender1.clone();
    let cast_executor = CastExecutor::new(plan.plan.schema().as_ref(), &plan.schema)?;
    let exec = async move {
        #[for_await]
        for record in data_stream {
            let res: Result<DataChunk, BoxedError> = async {
                let record = record?;
                if shutdown_rx.is_cancelled() {
                    Err(SchedulerError::QueryCancelled(
                        "Cancelled by user".to_owned(),
                    ))?;
                }
                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record)?;
                let chunk = cast_executor.execute(chunk).await?;
                Ok(chunk)
            }
            .await;
            if sender2.send(res).await.is_err() {
                tracing::info!("Receiver closed.");
                return;
            }
        }
    };
    let timeout = if cfg!(madsim) {
        None
    } else {
        Some(session.statement_timeout())
    };
    if let Some(timeout) = timeout {
        let exec = async move {
            if tokio::time::timeout(timeout, exec).await.is_ok() {
                return;
            }
            tracing::error!(
                "Datafusion query execution timeout after {} seconds",
                timeout.as_secs()
            );
            if sender1
                .send(Err(Box::new(SchedulerError::QueryCancelled(format!(
                    "timeout after {} seconds",
                    timeout.as_secs(),
                ))) as BoxedError))
                .await
                .is_err()
            {
                tracing::info!("Receiver closed.");
            }
        };
        compute_runtime.spawn(exec);
    } else {
        compute_runtime.spawn(exec);
    }

    let row_stream = PgResponseStream::LocalQuery(DataChunkToRowSetAdapter::new(
        receiver.into(),
        column_types,
        formats.clone(),
        session.clone(),
    ));

    let first_field_format = formats.first().copied().unwrap_or(Format::Text);

    Ok(PgResponse::builder(plan.stmt_type)
        .row_cnt_format_opt(Some(first_field_format))
        .values(row_stream, pg_descs)
        .into())
}

#[derive(Debug)]
pub struct CastExecutor {
    executors: Vec<Option<BoxedExpression>>,
}

impl CastExecutor {
    pub fn new(df_schema: &DFSchema, rw_schema: &RwSchema) -> RwResult<Self> {
        let mut executors = Vec::with_capacity(df_schema.fields().len());
        for (i, (df_field, rw_field)) in df_schema
            .fields()
            .iter()
            .zip_eq_fast(rw_schema.fields().iter())
            .enumerate()
        {
            let target_type = rw_field.data_type();
            let source_type = IcebergArrowConvert.type_from_field(df_field)?;

            if source_type == target_type {
                executors.push(None);
            } else {
                let cast_executor = build_single_cast_executor(i, source_type, target_type)?;
                executors.push(Some(cast_executor));
            }
        }
        Ok(CastExecutor { executors })
    }

    pub fn from_iter(
        source_types: impl ExactSizeIterator<Item = DFDataType>,
        target_types: impl ExactSizeIterator<Item = RwDataType>,
    ) -> RwResult<Self> {
        if source_types.len() != target_types.len() {
            return Err(ErrorCode::InternalError(format!(
                "source types length {} not equal to target types length {}",
                source_types.len(),
                target_types.len()
            ))
            .into());
        }
        let mut executors = Vec::with_capacity(target_types.len());
        for (i, (source_type, target_type)) in source_types.zip_eq_fast(target_types).enumerate() {
            let source_type =
                IcebergArrowConvert.type_from_field(&Field::new("", source_type.clone(), true))?;

            if source_type == target_type {
                executors.push(None);
            } else {
                let cast_executor = build_single_cast_executor(i, source_type, target_type)?;
                executors.push(Some(cast_executor));
            }
        }
        Ok(CastExecutor { executors })
    }

    pub async fn execute(&self, chunk: DataChunk) -> RwResult<DataChunk> {
        let mut arrays = Vec::with_capacity(chunk.columns().len());
        for (exe, col) in self.executors.iter().zip_eq_fast(chunk.columns()) {
            if let Some(exe) = exe {
                arrays.push(exe.eval(&chunk).await?);
            } else {
                arrays.push(col.clone());
            }
        }
        Ok(DataChunk::new(arrays, chunk.into_parts_v2().1))
    }
}

fn build_single_cast_executor(
    idx: usize,
    source_type: RwDataType,
    target_type: RwDataType,
) -> RwResult<BoxedExpression> {
    let expr: ExprImpl = InputRef::new(idx, source_type).into();
    let expr = expr.cast_explicit(&target_type)?;
    let res = build_from_prost(
        &expr
            .try_to_expr_proto()
            .map_err(ErrorCode::InvalidInputSyntax)?,
    )?;
    Ok(res)
}

fn create_config(session: &SessionImpl) -> DFSessionConfig {
    let rw_config = session.config();

    let mut df_config = DFSessionConfig::new();
    if let Some(batch_parallelism) = rw_config.batch_parallelism().0 {
        df_config = df_config.with_target_partitions(batch_parallelism.get().try_into().unwrap());
    }
    df_config = df_config.with_batch_size(session.env().batch_config().developer.chunk_size);
    df_config
}
