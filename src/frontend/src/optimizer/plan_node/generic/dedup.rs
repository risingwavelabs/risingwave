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
use risingwave_common::catalog::{FieldDisplay, Schema};

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Dedup<PlanRef> {
    pub input: PlanRef,
    /// Column indices of the columns to be deduplicated.
    pub dedup_cols: Vec<usize>,
}

impl<PlanRef: GenericPlanRef> Dedup<PlanRef> {
    pub fn new(input: PlanRef, dedup_cols: Vec<usize>) -> Self {
        debug_assert!(
            dedup_cols.iter().all(|&idx| idx < input.schema().len()),
            "Invalid dedup keys {:?} input schema size = {}",
            &dedup_cols,
            input.schema().len()
        );
        Dedup { input, dedup_cols }
    }

    fn dedup_cols_pretty<'a>(&self) -> Pretty<'a> {
        Pretty::Array(
            self.dedup_cols
                .iter()
                .map(|i| FieldDisplay(self.input.schema().fields.get(*i).unwrap()))
                .map(|fd| Pretty::display(&fd))
                .collect(),
        )
    }
}

impl<PlanRef: GenericPlanRef> DistillUnit for Dedup<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(name, vec![("dedup_cols", self.dedup_cols_pretty())])
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Dedup<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.dedup_cols.clone())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }
}
