use pretty_xmlish::StrAssocArr;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::{impl_distill_unit_from_fields, GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::ColPrunable;
use crate::optimizer::property::FunctionalDependencySet;
use crate::OptimizerContextRef;

/// `Union` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecursiveUnion<PlanRef> {
    pub base: PlanRef,
    pub recursive: PlanRef,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for RecursiveUnion<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        todo!()
    }

    fn schema(&self) -> Schema {
        let mut base = self.base.schema().clone();
        let iter_field = Field::with_name(DataType::Int16, "$iter");
        base.fields.push(iter_field);
        base
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let fields_len = self.base.schema().len();
        let base = self.base.stream_key();
        if let Some(base) = base {
            let mut base = base.to_vec();
            base.push(fields_len);
            Some(base)
        } else {
            None
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.base.ctx()
    }
}

impl<PlanRef: GenericPlanRef> RecursiveUnion<PlanRef> {
    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        vec![]
    }
}

impl_distill_unit_from_fields!(RecursiveUnion, GenericPlanRef);
