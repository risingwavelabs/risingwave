use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalAgg, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;

pub struct ApplyAgg {}
impl Rule for ApplyAgg {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        assert_eq!(apply.join_type(), JoinType::Inner);
        let right = apply.right();
        let agg = right.as_logical_agg()?;

        let apply_left_len = apply.left().schema().len();
        let mut group_keys: Vec<usize> = (0..apply_left_len).collect();
        let (mut agg_calls, agg_group_keys, _) = agg.clone().decompose();
        group_keys.extend(agg_group_keys.into_iter().map(|key| key + apply_left_len));
        // Shift index of agg_calls' input_ref with `apply_left_len`.
        agg_calls.iter_mut().for_each(|agg_call| {
            agg_call.inputs.iter_mut().for_each(|input_ref| {
                input_ref.shift_with_offset(apply_left_len as isize);
            });
        });

        let new_apply = apply.clone_with_left_right(apply.left(), agg.input());
        let new_agg = LogicalAgg::new(agg_calls, group_keys, new_apply.into());
        Some(new_agg.into())
    }
}

impl ApplyAgg {
    pub fn create() -> BoxedRule {
        Box::new(ApplyAgg {})
    }
}
