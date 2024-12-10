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

pub mod aggregation;
mod delete;
mod expand;
mod filter;
mod generic_exchange;
mod group_top_n;
mod hash_agg;
mod hop_window;
mod iceberg_scan;
mod insert;
mod join;
mod limit;
mod log_row_seq_scan;
mod managed;
mod max_one_row;
mod merge_sort;
mod merge_sort_exchange;
mod mysql_query;
mod order_by;
mod postgres_query;
mod project;
mod project_set;
mod row_seq_scan;
mod s3_file_scan;
mod sort_agg;
mod sort_over_window;
mod source;
mod sys_row_seq_scan;
mod table_function;
pub mod test_utils;
mod top_n;
mod union;
mod update;
mod utils;
mod values;

use std::future::Future;
use std::sync::Arc;

use anyhow::Context;
use async_recursion::async_recursion;
pub use delete::*;
pub use expand::*;
pub use filter::*;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
pub use generic_exchange::*;
pub use group_top_n::*;
pub use hash_agg::*;
pub use hop_window::*;
pub use iceberg_scan::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
pub use managed::*;
pub use max_one_row::*;
pub use merge_sort::*;
pub use merge_sort_exchange::*;
pub use mysql_query::*;
pub use order_by::*;
pub use postgres_query::*;
pub use project::*;
pub use project_set::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::plan_node::NodeBodyDiscriminants;
use risingwave_pb::batch_plan::PlanNode;
use risingwave_pb::common::BatchQueryEpoch;
pub use row_seq_scan::*;
pub use sort_agg::*;
pub use sort_over_window::SortOverWindowExecutor;
pub use source::*;
pub use table_function::*;
use thiserror_ext::AsReport;
pub use top_n::TopNExecutor;
pub use union::*;
pub use update::*;
pub use utils::*;
pub use values::*;

use self::log_row_seq_scan::LogStoreRowSeqScanExecutorBuilder;
use self::test_utils::{BlockExecutorBuilder, BusyLoopExecutorBuilder};
use crate::error::Result;
use crate::executor::s3_file_scan::FileScanExecutorBuilder;
use crate::executor::sys_row_seq_scan::SysRowSeqScanExecutorBuilder;
use crate::task::{BatchTaskContext, ShutdownToken, TaskId};

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedDataChunkStream = BoxStream<'static, Result<DataChunk>>;

pub struct ExecutorInfo {
    pub schema: Schema,
    pub id: String,
}

/// Refactoring of `Executor` using `Stream`.
pub trait Executor: Send + 'static {
    /// Returns the schema of the executor's return data.
    ///
    /// Schema must be available before `init`.
    fn schema(&self) -> &Schema;

    /// Identity string of the executor
    fn identity(&self) -> &str;

    /// Executes to return the data chunk stream.
    ///
    /// The implementation should guaranteed that each `DataChunk`'s cardinality is not zero.
    fn execute(self: Box<Self>) -> BoxedDataChunkStream;
}

impl std::fmt::Debug for BoxedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.identity())
    }
}

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor`
/// from proto and global environment.
pub trait BoxedExecutorBuilder {
    fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> impl Future<Output = Result<BoxedExecutor>> + Send;
}

pub struct ExecutorBuilder<'a> {
    pub plan_node: &'a PlanNode,
    pub task_id: &'a TaskId,
    context: Arc<dyn BatchTaskContext>,
    epoch: BatchQueryEpoch,
    shutdown_rx: ShutdownToken,
}

impl<'a> ExecutorBuilder<'a> {
    pub fn new(
        plan_node: &'a PlanNode,
        task_id: &'a TaskId,
        context: Arc<dyn BatchTaskContext>,
        epoch: BatchQueryEpoch,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        Self {
            plan_node,
            task_id,
            context,
            epoch,
            shutdown_rx,
        }
    }

    #[must_use]
    pub fn clone_for_plan(&self, plan_node: &'a PlanNode) -> Self {
        ExecutorBuilder::new(
            plan_node,
            self.task_id,
            self.context.clone(),
            self.epoch,
            self.shutdown_rx.clone(),
        )
    }

    pub fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub fn context(&self) -> &Arc<dyn BatchTaskContext> {
        &self.context
    }

    pub fn epoch(&self) -> BatchQueryEpoch {
        self.epoch
    }
}

/// Descriptor for executor builder.
///
/// We will call `builder` to build the executor if the `node_body` matches.
pub struct ExecutorBuilderDescriptor {
    pub node_body: NodeBodyDiscriminants,

    /// Typically from [`BoxedExecutorBuilder::new_boxed_executor`].
    pub builder: for<'a> fn(
        source: &'a ExecutorBuilder<'a>,
        inputs: Vec<BoxedExecutor>,
    ) -> BoxFuture<'a, Result<BoxedExecutor>>,
}

/// All registered executor builders.
#[linkme::distributed_slice]
pub static BUILDER_DESCS: [ExecutorBuilderDescriptor];

/// Register an executor builder so that it can be used to build the executor from protobuf.
macro_rules! register_executor {
    ($node_body:ident, $builder:ty) => {
        const _: () = {
            use futures::FutureExt;
            use risingwave_pb::batch_plan::plan_node::NodeBodyDiscriminants;

            use crate::executor::{ExecutorBuilderDescriptor, BUILDER_DESCS};

            #[linkme::distributed_slice(BUILDER_DESCS)]
            static BUILDER: ExecutorBuilderDescriptor = ExecutorBuilderDescriptor {
                node_body: NodeBodyDiscriminants::$node_body,
                builder: |a, b| <$builder>::new_boxed_executor(a, b).boxed(),
            };
        };
    };
}
pub(crate) use register_executor;

register_executor!(RowSeqScan, RowSeqScanExecutorBuilder);
register_executor!(Insert, InsertExecutor);
register_executor!(Delete, DeleteExecutor);
register_executor!(Exchange, GenericExchangeExecutorBuilder);
register_executor!(Update, UpdateExecutor);
register_executor!(Filter, FilterExecutor);
register_executor!(Project, ProjectExecutor);
register_executor!(SortAgg, SortAggExecutor);
register_executor!(Sort, SortExecutor);
register_executor!(TopN, TopNExecutor);
register_executor!(GroupTopN, GroupTopNExecutorBuilder);
register_executor!(Limit, LimitExecutor);
register_executor!(Values, ValuesExecutor);
register_executor!(NestedLoopJoin, NestedLoopJoinExecutor);
register_executor!(HashJoin, HashJoinExecutor<()>);
register_executor!(HashAgg, HashAggExecutorBuilder);
register_executor!(MergeSortExchange, MergeSortExchangeExecutorBuilder);
register_executor!(TableFunction, TableFunctionExecutorBuilder);
register_executor!(HopWindow, HopWindowExecutor);
register_executor!(SysRowSeqScan, SysRowSeqScanExecutorBuilder);
register_executor!(Expand, ExpandExecutor);
register_executor!(LocalLookupJoin, LocalLookupJoinExecutorBuilder);
register_executor!(DistributedLookupJoin, DistributedLookupJoinExecutorBuilder);
register_executor!(ProjectSet, ProjectSetExecutor);
register_executor!(Union, UnionExecutor);
register_executor!(Source, SourceExecutor);
register_executor!(SortOverWindow, SortOverWindowExecutor);
register_executor!(MaxOneRow, MaxOneRowExecutor);
register_executor!(FileScan, FileScanExecutorBuilder);
register_executor!(IcebergScan, IcebergScanExecutorBuilder);
register_executor!(PostgresQuery, PostgresQueryExecutorBuilder);
register_executor!(MysqlQuery, MySqlQueryExecutorBuilder);

// Following executors are only for testing.
register_executor!(BlockExecutor, BlockExecutorBuilder);
register_executor!(BusyLoopExecutor, BusyLoopExecutorBuilder);
register_executor!(LogRowSeqScan, LogStoreRowSeqScanExecutorBuilder);

impl<'a> ExecutorBuilder<'a> {
    pub async fn build(&self) -> Result<BoxedExecutor> {
        self.try_build()
            .await
            .inspect_err(|e| {
                let plan_node = self.plan_node.get_node_body();
                error!(error = %e.as_report(), ?plan_node, "failed to build executor");
            })
            .context("failed to build executor")
            .map_err(Into::into)
    }

    #[async_recursion]
    async fn try_build(&self) -> Result<BoxedExecutor> {
        let mut inputs = Vec::with_capacity(self.plan_node.children.len());
        for input_node in &self.plan_node.children {
            let input = self.clone_for_plan(input_node).build().await?;
            inputs.push(input);
        }

        let node_body_discriminants: NodeBodyDiscriminants =
            self.plan_node.get_node_body().unwrap().into();

        let builder = BUILDER_DESCS
            .iter()
            .find(|x| x.node_body == node_body_discriminants)
            .with_context(|| {
                format!(
                    "no executor builder found for {:?}",
                    node_body_discriminants
                )
            })?
            .builder;

        let real_executor = builder(self, inputs).await?;

        Ok(Box::new(ManagedExecutor::new(
            real_executor,
            self.shutdown_rx.clone(),
        )) as BoxedExecutor)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::test_batch_query_epoch;
    use risingwave_pb::batch_plan::PlanNode;

    use crate::executor::ExecutorBuilder;
    use crate::task::{ComputeNodeContext, ShutdownToken, TaskId};

    #[tokio::test]
    async fn test_clone_for_plan() {
        let plan_node = PlanNode::default();
        let task_id = &TaskId {
            task_id: 1,
            stage_id: 1,
            query_id: "test_query_id".to_owned(),
        };
        let builder = ExecutorBuilder::new(
            &plan_node,
            task_id,
            ComputeNodeContext::for_test(),
            test_batch_query_epoch(),
            ShutdownToken::empty(),
        );
        let child_plan = &PlanNode::default();
        let cloned_builder = builder.clone_for_plan(child_plan);
        assert_eq!(builder.task_id, cloned_builder.task_id);
    }
}
