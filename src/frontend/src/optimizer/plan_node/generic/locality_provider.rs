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

use std::fmt;

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef, impl_distill_unit_from_fields};
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

/// `LocalityProvider` provides locality for operators during backfilling.
/// It buffers input data into a state table using locality columns as primary key prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalityProvider<PlanRef> {
    pub input: PlanRef,
    /// Columns that define the locality
    pub locality_columns: Vec<usize>,
}

impl<PlanRef: GenericPlanRef> fmt::Display for LocalityProvider<PlanRef> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LocalityProvider {{ locality_columns: {:?} }}",
            self.locality_columns
        )
    }
}

impl<PlanRef: GenericPlanRef> LocalityProvider<PlanRef> {
    pub fn new(input: PlanRef, locality_columns: Vec<usize>) -> Self {
        Self {
            input,
            locality_columns,
        }
    }

    pub fn fields_pretty<'a>(&self) -> Vec<(&'a str, pretty_xmlish::Pretty<'a>)> {
        let locality_columns_str = format!("{:?}", self.locality_columns);
        vec![("locality_columns", locality_columns_str.into())]
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for LocalityProvider<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.input.stream_key()?.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }
}

impl<PlanRef: GenericPlanRef> LocalityProvider<PlanRef> {
    pub fn rewrite_exprs(&mut self, _r: &mut dyn ExprRewriter) {
        // LocalityProvider doesn't contain expressions to rewrite
    }

    pub fn visit_exprs(&self, _v: &mut dyn crate::expr::ExprVisitor) {
        // LocalityProvider doesn't contain expressions to visit
    }
}

impl_distill_unit_from_fields!(LocalityProvider, GenericPlanRef);
