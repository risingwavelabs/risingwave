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

use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::Schema;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

/// `Except` returns the rows of its first input except any
///  matching rows from its other inputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Except<PlanRef> {
    pub all: bool,
    pub inputs: Vec<PlanRef>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Except<PlanRef> {
    fn schema(&self) -> Schema {
        self.inputs[0].schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.inputs[0].stream_key()?.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.inputs[0].ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.inputs[0].schema().len())
    }
}

impl<PlanRef> DistillUnit for Except<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(name, vec![("all", Pretty::debug(&self.all))])
    }
}
