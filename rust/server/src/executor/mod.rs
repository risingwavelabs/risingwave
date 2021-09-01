mod create_table;
use create_table::*;
mod insert_values;
use insert_values::*;
mod drop_table;
use drop_table::*;
mod seq_scan;
use seq_scan::*;
mod filter;
use filter::*;

#[cfg(test)]
mod test_utils;

use crate::array::DataChunkRef;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::task::GlobalTaskEnv;
use risingwave_proto::plan::{PlanNode, PlanNode_PlanNodeType};
use std::convert::TryFrom;

pub(crate) enum ExecutorResult {
    Batch(DataChunkRef),
    Done,
}

impl ExecutorResult {
    #[cfg(test)] // Remove when this is useful in non-test code.
    fn batch_or(&self) -> Result<DataChunkRef> {
        match self {
            ExecutorResult::Batch(chunk) => Ok(chunk.clone()),
            ExecutorResult::Done => {
                Err(InternalError("result is Done, not Batch".to_string()).into())
            }
        }
    }
}

pub(crate) trait Executor: Send {
    fn init(&mut self) -> Result<()>;
    fn execute(&mut self) -> Result<ExecutorResult>;
    fn clean(&mut self) -> Result<()>;
}

pub(crate) type BoxedExecutor = Box<dyn Executor>;

pub(crate) struct ExecutorBuilder<'a> {
    plan_node: &'a PlanNode,
    env: GlobalTaskEnv,
}

macro_rules! build_executor {
  ($source: expr, $($proto_type_name:path => $data_type:ty),*) => {
    match $source.plan_node().get_node_type() {
      $(
        $proto_type_name => {
          <$data_type>::try_from($source).map(|d| Box::new(d) as BoxedExecutor)
        },
      )*
      _ => Err(RwError::from(InternalError(format!("Unsupported plan node type: {:?}", $source.plan_node().get_node_type()))))
    }
  }
}

impl<'a> ExecutorBuilder<'a> {
    pub(crate) fn new(plan_node: &'a PlanNode, env: GlobalTaskEnv) -> Self {
        Self { plan_node, env }
    }

    pub(crate) fn build(&self) -> Result<BoxedExecutor> {
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
          PlanNode_PlanNodeType::INSERT_VALUE => InsertValuesExecutor,
          PlanNode_PlanNodeType::DROP_TABLE => DropTableExecutor,
          PlanNode_PlanNodeType::FILTER => FilterExecutor
        }
    }

    pub(crate) fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub(crate) fn global_task_env(&self) -> &GlobalTaskEnv {
        &self.env
    }
}
