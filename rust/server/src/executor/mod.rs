mod create_table;
use create_table::*;
mod insert_values;
use insert_values::*;
mod seq_scan;
use seq_scan::*;

use crate::array::DataChunkRef;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::error::RwError;
use crate::task::TaskContext;
use risingwave_proto::plan::PlanNode_PlanNodeType;
use risingwave_proto::plan::{PlanFragment, PlanNode};
use std::convert::TryFrom;

pub(crate) enum ExecutorResult {
    Batch(DataChunkRef),
    Done,
}

pub(crate) trait Executor: Send {
    fn init(&mut self) -> Result<()>;
    fn execute(&mut self) -> Result<ExecutorResult>;
    fn clean(&mut self) -> Result<()>;
}

pub(crate) type BoxedExecutor = Box<dyn Executor>;

pub(crate) struct ExecutorBuilder<'a> {
    plan_node: &'a PlanNode,
    task_context: TaskContext,
}

macro_rules! build_executor {
  ($source: expr, $($proto_type_name:path => $data_type:ty),*) => {
    match $source.plan_node().get_node_type() {
      $(
        $proto_type_name => {
          <$data_type>::try_from($source).map(|d| Box::new(d) as BoxedExecutor)
        },
      )*
      _ => Err(RwError::from(InternalError(format!("Unsupported expression type: {:?}", $source.plan_node().get_node_type()))))
    }
  }
}

pub(crate) fn create_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
    build_executor! { source,
      PlanNode_PlanNodeType::CREATE_TABLE => CreateTableExecutor,
      PlanNode_PlanNodeType::SEQ_SCAN => SeqScanExecutor,
      PlanNode_PlanNodeType::INSERT_VALUE => InsertValuesExecutor
    }
}

impl<'a> ExecutorBuilder<'a> {
    pub(crate) fn new(plan_node: &'a PlanNode, task_context: TaskContext) -> Self {
        Self {
            plan_node,
            task_context,
        }
    }

    pub(crate) fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub(crate) fn task_context(&self) -> &TaskContext {
        &self.task_context
    }
}

/// Transform the PlanFragment into a tree of executors.
/// Each executor represents a physical operator, e.g. FilterScan, SeqScan.
pub(crate) fn transform_plan_tree(plan: &PlanFragment) -> Result<BoxedExecutor> {
    return transform_plan_node(plan.get_root());
}

fn transform_plan_node(_root: &PlanNode) -> Result<BoxedExecutor> {
    // match root.get_node_type() {
    //   PlanNodeType::SEQ_SCAN => SeqScanExecutor::try_from(root),
    //   _ => Err(
    //     ErrorCode::NotImplementedError(format!(
    //       "unsupported plan node type {:?}",
    //       root.get_node_type()
    //     ))
    //     .into(),
    //   ),
    // }
    todo!()
}
