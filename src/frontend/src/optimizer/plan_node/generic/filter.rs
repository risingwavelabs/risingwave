use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::session::OptimizerContextRef;
use crate::utils::Condition;

/// [`Filter`] iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone)]
pub struct Filter<PlanRef> {
    pub predicate: Condition,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Filter<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}
