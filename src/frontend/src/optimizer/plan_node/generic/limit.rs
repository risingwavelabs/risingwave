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
use std::hash::Hash;

use pretty_xmlish::Pretty;
use risingwave_common::catalog::Schema;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Limit<PlanRef> {
    pub input: PlanRef,
    pub limit: u64,
    pub offset: u64,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Limit<PlanRef> {
    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }
}
impl<PlanRef> Limit<PlanRef> {
    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ limit: {}, offset: {} }}",
            name, self.limit, self.offset
        )
    }

    pub fn new(input: PlanRef, limit: u64, offset: u64) -> Self {
        Limit {
            input,
            limit,
            offset,
        }
    }
}

impl<PlanRef> DistillUnit for Limit<PlanRef> {
    fn distill_with_name<'a>(&self, name: &'a str) -> Pretty<'a> {
        Pretty::childless_record(
            name,
            vec![
                ("limit", Pretty::debug(&self.limit)),
                ("offset", Pretty::debug(&self.offset)),
            ],
        )
    }
}
