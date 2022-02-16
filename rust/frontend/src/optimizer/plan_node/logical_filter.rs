use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, IntoPlanRef, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::expr::{assert_input_ref, ExprImpl};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};
use crate::utils::Condition;

#[derive(Debug, Clone)]
pub struct LogicalFilter {
    predicate: Condition,
    input: PlanRef,
    schema: Schema,
}
impl LogicalFilter {
    pub fn new(input: PlanRef, predicate: Condition) -> Self {
        for cond in &predicate.conjunctions {
            assert_input_ref(cond, input.schema().fields().len());
        }
        let schema = input.schema().clone();
        LogicalFilter {
            input,
            schema,
            predicate,
        }
    }

    /// the function will check if the predicate is bool expression
    pub fn create(input: PlanRef, predicate: ExprImpl) -> Result<PlanRef> {
        let predicate = Condition::with_expr(predicate);
        Ok(Self::new(input, predicate).into_plan_ref())
    }
}
impl PlanTreeNodeUnary for LogicalFilter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.predicate.clone())
    }
}
impl_plan_tree_node_for_unary! {LogicalFilter}
impl fmt::Display for LogicalFilter {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl WithOrder for LogicalFilter {}
impl WithDistribution for LogicalFilter {}
impl WithSchema for LogicalFilter {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
impl ColPrunable for LogicalFilter {}
impl ToBatch for LogicalFilter {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}
impl ToStream for LogicalFilter {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}
