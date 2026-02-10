// Copyright 2026 RisingWave Labs
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
use std::time::Instant;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::{SessionConfig as DFSessionConfig, SessionContext as DFSessionContext};
use futures_async_stream::for_await;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Format;
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::Schema as RwSchema;
use risingwave_common::error::BoxedError;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::PgResponseStream;
use crate::datafusion::CastExecutor;
use crate::datafusion::execute::memory_ctx::RwMemoryPool;
use crate::datafusion::execute::query_planner::RwCustomQueryPlanner;
use crate::error::{Result as RwResult, RwError};
use crate::handler::RwPgResponse;
use crate::handler::util::{DataChunkToRowSetAdapter, to_pg_field};
use crate::optimizer::BatchOptimizedLogicalPlanRoot;
use crate::scheduler::SchedulerError;
use crate::session::SessionImpl;
use crate::utils::DropGuard;

mod memory_ctx;
mod query_planner;

#[derive(Clone)]
pub struct DfBatchQueryPlanResult {
    pub(crate) plan: Arc<LogicalPlan>,
    pub(crate) schema: RwSchema,
    pub(crate) stmt_type: StatementType,
}

#[derive(Debug, Error)]
pub enum GenDataFusionPlanError {
    #[error("Missing Iceberg Scan")]
    MissingIcebergScan,
    #[error("Unsupported Plan Node")]
    UnsupportedPlanNode,
    #[error("Generating plan error: {0}")]
    Generating(RwError),
}

pub fn try_gen_datafusion_plan(
    optimized_logical: &BatchOptimizedLogicalPlanRoot,
) -> Result<Arc<LogicalPlan>, GenDataFusionPlanError> {
    use crate::optimizer::DataFusionExecuteCheckerExt;

    let check_result = optimized_logical.plan.check_for_datafusion();
    if !check_result.have_iceberg_scan {
        return Err(GenDataFusionPlanError::MissingIcebergScan);
    }
    if !check_result.supported {
        return Err(GenDataFusionPlanError::UnsupportedPlanNode);
    }

    let plan = optimized_logical
        .gen_datafusion_logical_plan()
        .map_err(GenDataFusionPlanError::Generating)?;
    Ok(plan)
}

pub fn create_datafusion_context(session: &SessionImpl) -> RwResult<DFSessionContext> {
    let df_config = create_config(session);
    let memory_pool = Arc::new(RwMemoryPool::new(session.env().mem_context()));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .build_arc()?;
    let state = SessionStateBuilder::new()
        .with_config(df_config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_query_planner(RwCustomQueryPlanner::new())
        .build();
    Ok(DFSessionContext::new_with_state(state))
}

pub async fn build_datafusion_physical_plan(
    ctx: &DFSessionContext,
    plan: &DfBatchQueryPlanResult,
) -> RwResult<Arc<dyn ExecutionPlan>> {
    let state = ctx.state();

    let df_plan = state.analyzer().execute_and_check(
        plan.plan.as_ref().clone(),
        state.config_options(),
        |_, _| {},
    )?;
    let df_plan = state.optimizer().optimize(df_plan, &state, |_, _| {})?;
    let physical_plan = state
        .query_planner()
        .create_physical_plan(&df_plan, &state)
        .await?;
    Ok(physical_plan)
}

pub async fn execute_datafusion_plan(
    session: Arc<SessionImpl>,
    plan: DfBatchQueryPlanResult,
    formats: Vec<Format>,
) -> RwResult<RwPgResponse> {
    let ctx = create_datafusion_context(session.as_ref())?;

    let query_start_time = Instant::now();

    let pg_descs: Vec<PgFieldDescriptor> = plan.schema.fields().iter().map(to_pg_field).collect();
    let column_types = plan.schema.fields().iter().map(|f| f.data_type()).collect();

    let physical_plan = build_datafusion_physical_plan(&ctx, &plan).await?;
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
                "DataFusion query execution timeout after {} seconds",
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

    let failed_counter_guard = DropGuard::new({
        let failed_query_counter = session
            .env()
            .frontend_metrics
            .datafusion
            .failed_query_counter
            .clone();
        move || {
            failed_query_counter.inc();
        }
    });
    let callback = async move {
        session
            .env()
            .frontend_metrics
            .datafusion
            .latency
            .observe(query_start_time.elapsed().as_secs_f64());

        session
            .env()
            .frontend_metrics
            .datafusion
            .completed_query_counter
            .inc();
        failed_counter_guard.disarm();

        Ok(())
    };

    Ok(PgResponse::builder(plan.stmt_type)
        .row_cnt_format_opt(Some(first_field_format))
        .values(row_stream, pg_descs)
        .callback(callback)
        .into())
}

fn create_config(session: &SessionImpl) -> DFSessionConfig {
    let rw_config = session.config();

    let mut df_config = DFSessionConfig::new();
    if let Some(batch_parallelism) = rw_config.batch_parallelism().0 {
        df_config = df_config.with_target_partitions(batch_parallelism.get().try_into().unwrap());
    }
    df_config = df_config.with_batch_size(session.env().batch_config().developer.chunk_size);
    df_config.options_mut().optimizer.prefer_hash_join = rw_config.datafusion_prefer_hash_join();

    df_config
}
