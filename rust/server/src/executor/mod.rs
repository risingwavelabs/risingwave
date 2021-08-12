mod seqscan;

use crate::array::DataChunk;
use crate::error::{ErrorCode, Result};
use crate::executor::seqscan::SeqScanExecutor;
use risingwave_proto::plan::PlanNode_PlanNodeType as PlanNodeType;
use risingwave_proto::plan::{PlanFragment, PlanNode};

pub(crate) enum ExecutorResult {
    Batch(DataChunk),
    Done,
}

pub(crate) trait Executor: Send {
    fn init(&mut self) -> Result<()>;
    fn execute(&mut self) -> Result<ExecutorResult>;
    fn clean(&mut self) -> Result<()>;
}

pub(crate) type BoxedExecutor = Box<dyn Executor>;

/// Transform the PlanFragment into a tree of executors.
/// Each executor represents a physical operator, e.g. FilterScan, SeqScan.
pub(crate) fn transform_plan_tree(plan: &PlanFragment) -> Result<BoxedExecutor> {
    return transform_plan_node(plan.get_root());
}

fn transform_plan_node(root: &PlanNode) -> Result<BoxedExecutor> {
    match root.get_node_type() {
        PlanNodeType::SEQ_SCAN => SeqScanExecutor::try_from(root),
        _ => Err(ErrorCode::NotImplementedError(format!(
            "unsupported plan node type {:?}",
            root.get_node_type()
        ))
        .into()),
    }
}
