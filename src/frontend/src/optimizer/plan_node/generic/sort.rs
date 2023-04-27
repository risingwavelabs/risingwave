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

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sort<PlanRef> {
    pub input: PlanRef,
    pub sort_column_index: usize,
}

impl<PlanRef: GenericPlanRef> Sort<PlanRef> {
    pub fn new(input: PlanRef, sort_column_index: usize) -> Self {
        Self {
            input,
            sort_column_index,
        }
    }
}

impl<PlanRef: GenericPlanRef> Sort<PlanRef> {
    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        f.debug_struct(name)
            .field("sort_column_index", &self.sort_column_index)
            .finish()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Sort<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }

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
