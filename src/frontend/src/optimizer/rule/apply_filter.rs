use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalFilter, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

pub struct ApplyFilter {}
impl Rule for ApplyFilter {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        assert_eq!(apply.join_type(), JoinType::Inner);
        let right = apply.right();
        let filter = right.as_logical_filter()?;

        let mut rewriter = Rewriter {
            offset: apply.left().schema().len(),
        };
        let predicate = filter
            .predicate()
            .clone()
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect();

        let new_apply = apply.clone_with_left_right(apply.left(), filter.input());
        let new_filter = LogicalFilter::create(
            new_apply.into(),
            Condition {
                conjunctions: predicate,
            },
        );
        Some(new_filter)
    }
}

impl ApplyFilter {
    pub fn create() -> BoxedRule {
        Box::new(ApplyFilter {})
    }
}

struct Rewriter {
    offset: usize,
}

impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        // Convert correlated_input_ref to input_ref.
        // TODO: use LiftCorrelatedInputRef here.
        InputRef::new(
            correlated_input_ref.index(),
            correlated_input_ref.return_type(),
        )
        .into()
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}
