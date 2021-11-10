use drop_stream::*;
use drop_table::*;
use filter::*;
use generic_exchange::*;
use hash_agg::*;
use limit::*;
use order_by::*;
use projection::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::plan::{plan_node::PlanNodeType, PlanNode};
use row_seq_scan::*;
use seq_scan::*;
use sort_agg::*;
use top_n::*;

use crate::executor::create_stream::CreateStreamExecutor;
use crate::executor::create_table::CreateTableExecutor;
use crate::executor::insert::InsertExecutor;
use crate::executor::join::nested_loop_join::NestedLoopJoinExecutor;
use crate::executor::join::HashJoinExecutorBuilder;
pub use crate::executor::stream_scan::StreamScanExecutor;
use crate::executor::values::ValuesExecutor;
use crate::task::{GlobalTaskEnv, TaskId};

mod create_stream;
mod create_table;
mod drop_stream;
mod drop_table;
mod filter;
mod generic_exchange;
mod hash_agg;
mod insert;
mod join;
mod limit;
mod merge_sort_exchange;
mod order_by;
mod projection;
mod row_seq_scan;
mod seq_scan;
mod sort_agg;
mod stream_scan;
#[cfg(test)]
mod test_utils;
mod top_n;
mod values;

pub enum ExecutorResult {
    Batch(DataChunk),
    Done,
}

impl ExecutorResult {
    #[cfg(test)] // Remove when this is useful in non-test code.
    fn batch_or(self) -> Result<DataChunk> {
        match self {
            ExecutorResult::Batch(chunk) => Ok(chunk),
            ExecutorResult::Done => {
                Err(InternalError("result is Done, not Batch".to_string()).into())
            }
        }
    }
}

#[async_trait::async_trait]
pub trait Executor: Send {
    fn init(&mut self) -> Result<()>;
    async fn execute(&mut self) -> Result<ExecutorResult>;
    fn clean(&mut self) -> Result<()>;
    /// this method will return the schema of the executor's return data
    fn schema(&self) -> &Schema;
}

pub type BoxedExecutor = Box<dyn Executor>;

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor` from
/// proto and global environment
pub trait BoxedExecutorBuilder {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor>;
}

pub struct ExecutorBuilder<'a> {
    plan_node: &'a PlanNode,
    task_id: &'a TaskId,
    env: GlobalTaskEnv,
}

macro_rules! build_executor {
  ($source: expr, $($proto_type_name:path => $data_type:ty),*) => {
    match $source.plan_node().get_node_type() {
      $(
        $proto_type_name => {
          <$data_type>::new_boxed_executor($source)
        },
      )*
      _ => Err(RwError::from(InternalError(format!("Unsupported plan node type: {:?}", $source.plan_node().get_node_type()))))
    }
  }
}

impl<'a> ExecutorBuilder<'a> {
    pub fn new(plan_node: &'a PlanNode, task_id: &'a TaskId, env: GlobalTaskEnv) -> Self {
        Self {
            plan_node,
            task_id,
            env,
        }
    }

    pub fn build(&self) -> Result<BoxedExecutor> {
        self.try_build().map_err(|e| {
            InternalError(format!(
                "[PlanNodeType: {:?}] Failed to build executor: {}",
                self.plan_node.get_node_type(),
                e,
            ))
            .into()
        })
    }

    pub fn clone_for_plan(&self, plan_node: &'a PlanNode) -> Self {
        ExecutorBuilder::new(plan_node, self.task_id, self.env.clone())
    }

    fn try_build(&self) -> Result<BoxedExecutor> {
        build_executor! { self,
          PlanNodeType::CreateTable => CreateTableExecutor,
          PlanNodeType::SeqScan => SeqScanExecutor,
          PlanNodeType::RowSeqScan => RowSeqScanExecutor,
          PlanNodeType::Insert => InsertExecutor,
          PlanNodeType::DropTable => DropTableExecutor,
          PlanNodeType::Exchange => ExchangeExecutor,
          PlanNodeType::Filter => FilterExecutor,
          PlanNodeType::Project => ProjectionExecutor,
          PlanNodeType::SortAgg => SortAggExecutor,
          PlanNodeType::OrderBy => OrderByExecutor,
          PlanNodeType::CreateStream => CreateStreamExecutor,
          PlanNodeType::StreamScan => StreamScanExecutor,
          PlanNodeType::TopN => TopNExecutor,
          PlanNodeType::Limit => LimitExecutor,
          PlanNodeType::Value => ValuesExecutor,
          PlanNodeType::NestedLoopJoin => NestedLoopJoinExecutor,
          PlanNodeType::HashJoin => HashJoinExecutorBuilder,
          PlanNodeType::DropStream => DropStreamExecutor,
          PlanNodeType::HashAgg => HashAggExecutorBuilder
        }
    }

    pub fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub fn global_task_env(&self) -> &GlobalTaskEnv {
        &self.env
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan::PlanNode;

    use crate::executor::ExecutorBuilder;
    use crate::task::{GlobalTaskEnv, TaskId};

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
        let builder = ExecutorBuilder::new(&plan_node, task_id, GlobalTaskEnv::for_test());
        let child_plan = &PlanNode {
            ..Default::default()
        };
        let cloned_builder = builder.clone_for_plan(child_plan);
        assert_eq!(builder.task_id, cloned_builder.task_id);
    }
}
