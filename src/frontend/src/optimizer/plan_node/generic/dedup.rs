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

use itertools::Itertools;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{FieldDisplay, Schema};

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::utils::childless_record;
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
        builder.field("dedup_cols", &self.dedup_cols_display());
        builder.finish()
    }

    fn dedup_cols_display(&self) -> Vec<FieldDisplay<'_>> {
        self.dedup_cols
            .iter()
            .map(|i| FieldDisplay(self.input.schema().fields.get(*i).unwrap()))
            .collect_vec()
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
