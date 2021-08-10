use crate::error::Result;
use crate::executor::BoxedExecutor;
use risingwave_proto::plan::{PlanNode, PlanNode_PlanNodeType};

pub(super) struct SeqScanExecutor {}

impl SeqScanExecutor {
    pub(super) fn try_from(plan_node: &PlanNode) -> Result<BoxedExecutor> {
        ensure!(plan_node.get_node_type() == PlanNode_PlanNodeType::SEQ_SCAN);

        // let seq_scan_node = SeqScanNode::parse_from_bytes(plan_node.get_body().get_value())?;
        unimplemented!()
    }
}
