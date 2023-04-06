use std::fmt;

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Dedup<PlanRef> {
    pub input: PlanRef,
    /// Column indices of the columns to be deduplicated.
    pub dedup_cols: Vec<usize>,
}

impl<PlanRef: GenericPlanRef> Dedup<PlanRef> {
    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        builder.field("dedup_cols", &self.dedup_cols);
        builder.finish()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Dedup<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.dedup_cols.clone())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }
}
