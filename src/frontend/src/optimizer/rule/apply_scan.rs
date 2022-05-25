use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalJoin, PlanTreeNodeBinary};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

pub struct ApplyScan {}
impl Rule for ApplyScan {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        assert_eq!(apply.join_type(), JoinType::Inner);
        let right = apply.right();
        let _scan = right.as_logical_scan()?;

        let join = LogicalJoin::new(apply.left(), right, JoinType::Inner, Condition::true_cond());
        Some(join.into())
    }
}

impl ApplyScan {
    pub fn create() -> BoxedRule {
        Box::new(ApplyScan {})
    }
}
