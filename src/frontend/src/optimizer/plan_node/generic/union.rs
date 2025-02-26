// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef, impl_distill_unit_from_fields};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

/// `Union` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let mut schema = self.inputs[0].schema().clone();
        if let Some(source_col) = self.source_col {
            schema.fields[source_col].name = "$src".to_owned();
            schema
        } else {
            schema
        }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        // Union all its inputs pks + source_col if exists
        let mut pk_indices = vec![];
        for input in &self.inputs {
            for pk in input.stream_key()? {
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

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.inputs[0].schema().len())
    }
}

impl<PlanRef: GenericPlanRef> Union<PlanRef> {
    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        vec![("all", Pretty::debug(&self.all))]
    }
}
impl_distill_unit_from_fields!(Union, GenericPlanRef);
