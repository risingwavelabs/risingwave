// Copyright 2022 Singularity Data
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

use drop_stream::*;
use drop_table::*;
use generic_exchange::*;
use hash_agg::*;
use limit::*;
use merge_sort_exchange::*;
use order_by::*;
use projection::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::PlanNode;
pub use row_seq_scan::*;
use sort_agg::*;
use top_n::*;

use self::fuse::FusedExecutor;
use crate::executor::create_source::CreateSourceExecutor;
pub use crate::executor::create_table::CreateTableExecutor;
pub use crate::executor::delete::DeleteExecutor;
use crate::executor::generate_series::GenerateSeriesI32Executor;
pub use crate::executor::insert::InsertExecutor;
use crate::executor::join::nested_loop_join::NestedLoopJoinExecutor;
use crate::executor::join::sort_merge_join::SortMergeJoinExecutor;
use crate::executor::join::HashJoinExecutorBuilder;
use crate::executor::stream_scan::StreamScanExecutor;
use crate::executor::trace::TraceExecutor;
use crate::executor2::executor_wrapper::ExecutorWrapper;
use crate::executor2::{
    BoxedExecutor2, BoxedExecutor2Builder, FilterExecutor2, TraceExecutor2, ValuesExecutor2,
};
use crate::task::{BatchEnvironment, TaskId};

mod create_source;
mod create_table;
mod delete;
mod drop_stream;
mod drop_table;
pub mod executor2_wrapper;
mod fuse;
mod generate_series;
mod generic_exchange;
mod hash_agg;
mod insert;
mod join;
mod limit;
mod merge_sort_exchange;
pub mod monitor;
mod order_by;
mod projection;
mod row_seq_scan;
mod sort_agg;
mod stream_scan;
pub mod test_utils;
mod top_n;
mod trace;

/// `Executor` is an operator in the query execution.
#[async_trait::async_trait]
pub trait Executor: Send {
    /// Initializes the executor.
    async fn open(&mut self) -> Result<()>;

    /// Executes the executor to get next chunk
    async fn next(&mut self) -> Result<Option<DataChunk>>;

    /// Finalizes the executor.
    async fn close(&mut self) -> Result<()>;

    /// Returns the schema of the executor's return data.
    ///
    /// Schema must be available before `init`.
    fn schema(&self) -> &Schema;

    /// Identity string of the executor
    fn identity(&self) -> &str;

    /// Turn an executor into a fused executor
    fn fuse(self) -> FusedExecutor<Self>
    where
        Self: Executor + std::marker::Sized,
    {
        FusedExecutor::new(self)
    }
}

pub type BoxedExecutor = Box<dyn Executor>;

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor` from
/// proto and global environment
pub trait BoxedExecutorBuilder {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor>;

    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2> {
        Ok(Box::new(ExecutorWrapper::from(Self::new_boxed_executor(
            source,
        )?)))
    }
}

pub struct ExecutorBuilder<'a> {
    pub plan_node: &'a PlanNode,
    task_id: &'a TaskId,
    env: BatchEnvironment,
    epoch: u64,
}

macro_rules! build_executor {
    ($source: expr, $($proto_type_name:path => $data_type:ty),*) => {
        match $source.plan_node().get_node_body().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::new_boxed_executor($source)
                },
            )*
        }
    }
}

macro_rules! build_executor2 {
    ($source: expr, $($proto_type_name:path => $data_type:ty),*) => {
        match $source.plan_node().get_node_body().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::new_boxed_executor2($source)
                },
            )*
        }
    }
}

impl<'a> ExecutorBuilder<'a> {
    pub fn new(
        plan_node: &'a PlanNode,
        task_id: &'a TaskId,
        env: BatchEnvironment,
        epoch: u64,
    ) -> Self {
        Self {
            plan_node,
            task_id,
            env,
            epoch,
        }
    }

    pub fn build(&self) -> Result<BoxedExecutor> {
        self.try_build().map_err(|e| {
            InternalError(format!(
                "[PlanNode: {:?}] Failed to build executor: {}",
                self.plan_node.get_node_body(),
                e,
            ))
            .into()
        })
    }

    pub fn build2(&self) -> Result<BoxedExecutor2> {
        self.try_build2().map_err(|e| {
            InternalError(format!(
                "[PlanNode: {:?}] Failed to build executor: {}",
                self.plan_node.get_node_body(),
                e,
            ))
            .into()
        })
    }

    #[must_use]
    pub fn clone_for_plan(&self, plan_node: &'a PlanNode) -> Self {
        ExecutorBuilder::new(plan_node, self.task_id, self.env.clone(), self.epoch)
    }

    fn try_build(&self) -> Result<BoxedExecutor> {
        let real_executor = build_executor! { self,
            NodeBody::CreateTable => CreateTableExecutor,
            NodeBody::RowSeqScan => RowSeqScanExecutorBuilder,
            NodeBody::Insert => InsertExecutor,
            NodeBody::Delete => DeleteExecutor,
            NodeBody::DropTable => DropTableExecutor,
            NodeBody::Exchange => ExchangeExecutor,
            NodeBody::Filter => FilterExecutor2,
            NodeBody::Project => ProjectionExecutor,
            NodeBody::SortAgg => SortAggExecutor,
            NodeBody::OrderBy => OrderByExecutor,
            NodeBody::CreateSource => CreateSourceExecutor,
            NodeBody::SourceScan => StreamScanExecutor,
            NodeBody::TopN => TopNExecutor,
            NodeBody::Limit => LimitExecutor,
            NodeBody::Values => ValuesExecutor2,
            NodeBody::NestedLoopJoin => NestedLoopJoinExecutor,
            NodeBody::HashJoin => HashJoinExecutorBuilder,
            NodeBody::SortMergeJoin => SortMergeJoinExecutor,
            NodeBody::DropSource => DropStreamExecutor,
            NodeBody::HashAgg => HashAggExecutorBuilder,
            NodeBody::MergeSortExchange => MergeSortExchangeExecutor,
            NodeBody::GenerateInt32Series => GenerateSeriesI32Executor
        }?;
        let input_desc = real_executor.identity().to_string();
        Ok(Box::new(TraceExecutor::new(real_executor, input_desc)))
    }

    fn try_build2(&self) -> Result<BoxedExecutor2> {
        let real_executor = build_executor2! { self,
            NodeBody::CreateTable => CreateTableExecutor,
            NodeBody::RowSeqScan => RowSeqScanExecutorBuilder,
            NodeBody::Insert => InsertExecutor,
            NodeBody::Delete => DeleteExecutor,
            NodeBody::DropTable => DropTableExecutor,
            NodeBody::Exchange => ExchangeExecutor,
            NodeBody::Filter => FilterExecutor2,
            NodeBody::Project => ProjectionExecutor,
            NodeBody::SortAgg => SortAggExecutor,
            NodeBody::OrderBy => OrderByExecutor,
            NodeBody::CreateSource => CreateSourceExecutor,
            NodeBody::SourceScan => StreamScanExecutor,
            NodeBody::TopN => TopNExecutor,
            NodeBody::Limit => LimitExecutor,
            NodeBody::Values => ValuesExecutor2,
            NodeBody::NestedLoopJoin => NestedLoopJoinExecutor,
            NodeBody::HashJoin => HashJoinExecutorBuilder,
            NodeBody::SortMergeJoin => SortMergeJoinExecutor,
            NodeBody::DropSource => DropStreamExecutor,
            NodeBody::HashAgg => HashAggExecutorBuilder,
            NodeBody::MergeSortExchange => MergeSortExchangeExecutor,
            NodeBody::GenerateInt32Series => GenerateSeriesI32Executor
        }?;
        let input_desc = real_executor.identity().to_string();
        Ok(Box::new(TraceExecutor2::new(real_executor, input_desc)))
    }

    pub fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub fn global_batch_env(&self) -> &BatchEnvironment {
        &self.env
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan::PlanNode;

    use crate::executor::ExecutorBuilder;
    use crate::task::{BatchEnvironment, TaskId};

    #[test]
    fn test_clone_for_plan() {
        let plan_node = PlanNode {
            ..Default::default()
        };
        let task_id = &TaskId {
            task_id: 1,
            stage_id: 1,
            query_id: "test_query_id".to_string(),
        };
        let builder =
            ExecutorBuilder::new(&plan_node, task_id, BatchEnvironment::for_test(), u64::MAX);
        let child_plan = &PlanNode {
            ..Default::default()
        };
        let cloned_builder = builder.clone_for_plan(child_plan);
        assert_eq!(builder.task_id, cloned_builder.task_id);
    }
}
