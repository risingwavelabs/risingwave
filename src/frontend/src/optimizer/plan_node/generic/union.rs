



use itertools::Itertools;
use risingwave_common::catalog::{Schema};









use super::{GenericPlanNode, GenericPlanRef};




use crate::session::OptimizerContextRef;



/// `Union` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone)]
pub struct Union<PlanRef> {
    pub all: bool,
    pub inputs: Vec<PlanRef>,
    /// It is used by streaming processing. We need to use `source_col` to identify the record came
    /// from which source input.
    /// We add it as a logical property, because we need to derive the logical pk based on it.
    pub source_col: Option<usize>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Union<PlanRef> {
    fn schema(&self) -> Schema {
        self.inputs[0].schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        // Union all its inputs pks + source_col if exists
        let mut pk_indices = vec![];
        for input in &self.inputs {
            for pk in input.logical_pk() {
                if !pk_indices.contains(pk) {
                    pk_indices.push(*pk);
                }
            }
        }
        if let Some(source_col) = self.source_col {
            pk_indices.push(source_col)
        }
        Some(pk_indices)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.inputs[0].ctx()
    }
}
