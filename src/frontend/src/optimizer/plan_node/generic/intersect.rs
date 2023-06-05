// Copyright 2023 RisingWave Labs
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

use std::fmt;

use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::Schema;

use super::{impl_distill_unit_from_fields, GenericPlanNode, GenericPlanRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

/// `Intersect` returns the intersect of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Intersect<PlanRef> {
    pub all: bool,
    pub inputs: Vec<PlanRef>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Intersect<PlanRef> {
    fn schema(&self) -> Schema {
        self.inputs[0].schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.inputs[0].logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.inputs[0].ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.inputs[0].schema().len())
    }
}

impl<PlanRef: GenericPlanRef> Intersect<PlanRef> {
    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        self.fmt_fields_with_builder(&mut builder);
        builder.finish()
    }

    pub fn fmt_fields_with_builder(&self, builder: &mut fmt::DebugStruct<'_, '_>) {
        builder.field("all", &self.all);
    }

    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        vec![("all", Pretty::debug(&self.all))]
    }
}
impl_distill_unit_from_fields!(Intersect, GenericPlanRef);
