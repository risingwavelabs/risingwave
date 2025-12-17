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

use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig as DFSessionConfig, SessionContext as DFSessionContext};
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Format;
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::Schema;
use risingwave_common::error::BoxedError;
use risingwave_common::types::DataType;
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
    pub(crate) schema: Schema,
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

    // avoid optimizing by datafusion
    let physical_plan = state
        .query_planner()
        .create_physical_plan(&plan.plan, &state)
        .await?;
    let data_stream = execute_stream(physical_plan, ctx.task_ctx())?;

    let compute_runtime = session.env().compute_runtime();
    let (sender1, receiver) = mpsc::channel(10);
    let shutdown_rx = session.reset_cancel_query_flag();
    let sender2 = sender1.clone();
    let cast_executor = build_cast_executor(&plan.schema)?;
    let exec = async move {
        #[futures_async_stream::for_await]
        for record in data_stream {
            let res: std::result::Result<DataChunk, BoxedError> = async {
                let record = record?;
                if shutdown_rx.is_cancelled() {
                    Err(SchedulerError::QueryCancelled(
                        "Cancelled by user".to_owned(),
                    ))?;
                }
                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record)?;
                let chunk = cast_executor.execute(&chunk).await?;
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
            if tokio::time::timeout(timeout, exec).await.is_err() {
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

struct CastExecutor {
    executors: Vec<Option<BoxedExpression>>,
}

fn build_cast_executor(schema: &Schema) -> RwResult<CastExecutor> {
    let mut executors = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        let target_type = field.data_type();
        let source_type = IcebergArrowConvert
            .type_from_field(&IcebergArrowConvert.to_arrow_field("", &target_type)?)?;

        if source_type == target_type {
            executors.push(None);
        } else {
            let cast_executor = build_single_cast_executor(i, source_type, target_type)?;
            executors.push(Some(cast_executor));
        }
    }
    Ok(CastExecutor { executors })
}

fn build_single_cast_executor(
    idx: usize,
    source_type: DataType,
    target_type: DataType,
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

impl CastExecutor {
    pub async fn execute(&self, chunk: &DataChunk) -> RwResult<DataChunk> {
        let mut arrays = Vec::with_capacity(chunk.columns().len());
        for (exe, col) in self.executors.iter().zip_eq_fast(chunk.columns()) {
            if let Some(exe) = exe {
                arrays.push(exe.eval(chunk).await?);
            } else {
                arrays.push(col.clone());
            }
        }
        Ok(DataChunk::new(arrays, chunk.visibility().clone()))
    }
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
