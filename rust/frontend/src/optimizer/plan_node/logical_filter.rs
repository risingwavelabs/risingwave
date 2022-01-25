use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::types::DataTypeKind;

use super::{ColPrunable, IntoPlanRef, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::expr::{assert_input_ref, to_conjunctions, BoundExpr, BoundExprImpl};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalFilter {
    /// condition bool expressions, linked with AND conjunction
    conds: Vec<BoundExprImpl>,
    input: PlanRef,
    schema: Schema,
}
impl LogicalFilter {
    fn new(input: PlanRef, conds: Vec<BoundExprImpl>) -> Self {
        for cond in &conds {
            assert_eq!(cond.return_type(), DataTypeKind::Boolean);
            assert_input_ref(cond, input.schema().fields().len());
        }
        let schema = input.schema().clone();
        LogicalFilter {
            input,
            schema,
            conds,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, cond: BoundExprImpl) -> Result<PlanRef> {
        let conds = to_conjunctions(cond);
        Ok(Self::new(input, conds).into_plan_ref())
    }
}
impl PlanTreeNodeUnary for LogicalFilter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.conds.clone())
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
