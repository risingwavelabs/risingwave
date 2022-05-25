use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter, InputRef, Expr};
use crate::optimizer::plan_node::{LogicalProject, PlanTreeNodeBinary};
use crate::optimizer::PlanRef;

pub struct ApplyProj {}
impl Rule for ApplyProj {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        assert_eq!(apply.join_type(), JoinType::Inner);
        let right = apply.right();
        let project = right.as_logical_project()?;

        let mut exprs: Vec<ExprImpl> = apply
            .left()
            .schema()
            .data_types()
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| InputRef::new(index, data_type).into())
            .collect();

        let (proj_exprs, proj_input) = project.clone().decompose();
        // let mut shift_input_ref = ColIndexMapping::with_shift_offset(
        //     proj_exprs.len(),
        //     apply.left().schema().len() as isize,
        // );
        let mut rewriter = Rewriter {
            offset: apply.left().schema().len(),
        };
        exprs.extend(
            proj_exprs
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr)),
        );

        let new_apply = apply.clone_with_left_right(apply.left(), proj_input);
        let new_project = LogicalProject::create(new_apply.into(), exprs);
        Some(new_project)
    }
}

impl ApplyProj {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProj {})
    }
}

struct Rewriter {
    offset: usize,
}

impl ExprRewriter for Rewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}
