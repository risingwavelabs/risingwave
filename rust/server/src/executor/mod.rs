use limit::*;
use risingwave_proto::plan::{PlanNode, PlanNode_PlanNodeType};

use crate::array::DataChunk;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::executor::create_stream::CreateStreamExecutor;
use crate::executor::create_table::CreateTableExecutor;
use crate::executor::insert::InsertExecutor;
use crate::executor::join::nested_loop_join::NestedLoopJoinExecutor;
use crate::executor::join::HashJoinExecutorBuilder;
pub use crate::executor::stream_scan::StreamScanExecutor;
use crate::executor::values::ValuesExecutor;
use crate::task::GlobalTaskEnv;
use crate::types::DataTypeRef;
use drop_table::*;
use exchange::*;
use filter::*;
use order_by::*;
use projection::*;
use row_seq_scan::*;
use seq_scan::*;
use sort_agg::*;
use top_n::*;
mod drop_stream;
use drop_stream::*;

mod create_stream;
mod create_table;
mod drop_table;
mod exchange;
mod filter;
mod hash_map;
mod insert;
mod join;
mod limit;
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

/// the field in the schema of the executor's return data
#[derive(Clone)]
pub struct Field {
    // TODO: field_name
    data_type: DataTypeRef,
}

/// the schema of the executor's return data
pub struct Schema {
    fields: Vec<Field>,
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

/// every Executor should impl this trait to provide a static method to build a `BoxedExecutor` from proto and global environment
pub trait BoxedExecutorBuilder {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor>;
}

pub struct ExecutorBuilder<'a> {
    plan_node: &'a PlanNode,
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
    pub fn new(plan_node: &'a PlanNode, env: GlobalTaskEnv) -> Self {
        Self { plan_node, env }
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

    fn try_build(&self) -> Result<BoxedExecutor> {
        build_executor! { self,
          PlanNode_PlanNodeType::CREATE_TABLE => CreateTableExecutor,
          PlanNode_PlanNodeType::SEQ_SCAN => SeqScanExecutor,
          PlanNode_PlanNodeType::ROW_SEQ_SCAN => RowSeqScanExecutor,
          PlanNode_PlanNodeType::INSERT => InsertExecutor,
          PlanNode_PlanNodeType::DROP_TABLE => DropTableExecutor,
          PlanNode_PlanNodeType::EXCHANGE => ExchangeExecutor,
          PlanNode_PlanNodeType::FILTER => FilterExecutor,
          PlanNode_PlanNodeType::PROJECT => ProjectionExecutor,
          PlanNode_PlanNodeType::SORT_AGG => SortAggExecutor,
          PlanNode_PlanNodeType::ORDER_BY => OrderByExecutor,
          PlanNode_PlanNodeType::CREATE_STREAM => CreateStreamExecutor,
          PlanNode_PlanNodeType::STREAM_SCAN => StreamScanExecutor,
          PlanNode_PlanNodeType::TOP_N => TopNExecutor,
          PlanNode_PlanNodeType::LIMIT => LimitExecutor,
          PlanNode_PlanNodeType::VALUE => ValuesExecutor,
          PlanNode_PlanNodeType::NESTED_LOOP_JOIN => NestedLoopJoinExecutor,
          PlanNode_PlanNodeType::HASH_JOIN => HashJoinExecutorBuilder,
          PlanNode_PlanNodeType::DROP_STREAM => DropStreamExecutor
        }
    }

    pub fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub fn global_task_env(&self) -> &GlobalTaskEnv {
        &self.env
    }
}
