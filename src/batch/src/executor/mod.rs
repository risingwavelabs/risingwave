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

use risingwave_common::error::ErrorCode::{self, InternalError};
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::PlanNode;

use crate::executor2::{
    BoxedExecutor2, BoxedExecutor2Builder, DeleteExecutor2, FilterExecutor2,
    GenerateSeriesExecutor2Builder, GenericExchangeExecutor2Builder, HashAggExecutor2Builder,
    HashJoinExecutor2Builder, HopWindowExecutor2, InsertExecutor2, LimitExecutor2,
    MergeSortExchangeExecutor2Builder, NestedLoopJoinExecutor2, OrderByExecutor2, ProjectExecutor2,
    RowSeqScanExecutor2Builder, SortAggExecutor2, SortMergeJoinExecutor2, TopNExecutor2,
    TraceExecutor2, UpdateExecutor, ValuesExecutor2,
};
use crate::task::{BatchTaskContext, TaskId};

#[cfg(test)]
pub mod test_utils;

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor` from
/// proto and global environment
pub trait BoxedExecutorBuilder {
    fn new_boxed_executor2<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
    ) -> Result<BoxedExecutor2>;
}

#[allow(dead_code)]
struct NotImplementedBuilder;

impl BoxedExecutorBuilder for NotImplementedBuilder {
    fn new_boxed_executor2<C: BatchTaskContext>(
        _source: &ExecutorBuilder<C>,
    ) -> Result<BoxedExecutor2> {
        Err(ErrorCode::NotImplemented("Executor not implemented".to_string(), None.into()).into())
    }
}

pub struct ExecutorBuilder<'a, C> {
    pub plan_node: &'a PlanNode,
    pub task_id: &'a TaskId,
    context: C,
    epoch: u64,
}

macro_rules! build_executor2 {
    ($source: expr, $($proto_type_name:path => $data_type:ty),* $(,)?) => {
        match $source.plan_node().get_node_body().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::new_boxed_executor2($source)
                },
            )*
        }
    }
}

impl<'a, C: BatchTaskContext> ExecutorBuilder<'a, C> {
    pub fn new(plan_node: &'a PlanNode, task_id: &'a TaskId, context: C, epoch: u64) -> Self {
        Self {
            plan_node,
            task_id,
            context,
            epoch,
        }
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
        ExecutorBuilder::new(plan_node, self.task_id, self.context.clone(), self.epoch)
    }

    fn try_build2(&self) -> Result<BoxedExecutor2> {
        let real_executor = build_executor2! { self,
            NodeBody::RowSeqScan => RowSeqScanExecutor2Builder,
            NodeBody::Insert => InsertExecutor2,
            NodeBody::Delete => DeleteExecutor2,
            NodeBody::Exchange => GenericExchangeExecutor2Builder,
            NodeBody::Update => UpdateExecutor,
            NodeBody::Filter => FilterExecutor2,
            NodeBody::Project => ProjectExecutor2,
            NodeBody::SortAgg => SortAggExecutor2,
            NodeBody::OrderBy => OrderByExecutor2,
            NodeBody::TopN => TopNExecutor2,
            NodeBody::Limit => LimitExecutor2,
            NodeBody::Values => ValuesExecutor2,
            NodeBody::NestedLoopJoin => NestedLoopJoinExecutor2,
            NodeBody::HashJoin => HashJoinExecutor2Builder,
            NodeBody::SortMergeJoin => SortMergeJoinExecutor2,
            NodeBody::HashAgg => HashAggExecutor2Builder,
            NodeBody::MergeSortExchange => MergeSortExchangeExecutor2Builder,
            NodeBody::GenerateSeries => GenerateSeriesExecutor2Builder,
            NodeBody::HopWindow => HopWindowExecutor2,
        }?;
        let input_desc = real_executor.identity().to_string();
        Ok(Box::new(TraceExecutor2::new(real_executor, input_desc)))
    }

    pub fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub fn batch_task_context(&self) -> &C {
        &self.context
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::batch_plan::PlanNode;

    use crate::executor::ExecutorBuilder;
    use crate::task::{ComputeNodeContext, TaskId};

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
        let builder = ExecutorBuilder::new(
            &plan_node,
            task_id,
            ComputeNodeContext::new_for_test(),
            u64::MAX,
        );
        let child_plan = &PlanNode {
            ..Default::default()
        };
        let cloned_builder = builder.clone_for_plan(child_plan);
        assert_eq!(builder.task_id, cloned_builder.task_id);
    }
}
