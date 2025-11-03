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

use pretty_xmlish::Pretty;
use risingwave_common::catalog::{FieldDisplay, Schema};

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

impl<PlanRef: GenericPlanRef> LocalityProvider<PlanRef> {
    pub fn new(input: PlanRef, locality_columns: Vec<usize>) -> Self {
        Self {
            input,
            locality_columns,
        }
    }

    pub fn fields_pretty<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        let locality_columns_display = self
            .locality_columns
            .iter()
            .map(|&i| Pretty::display(&FieldDisplay(self.input.schema().fields.get(i).unwrap())))
            .collect();
        vec![("locality_columns", Pretty::Array(locality_columns_display))]
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for LocalityProvider<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let mut key_columns = self.locality_columns.clone();
        if let Some(input_stream_key) = self.input.stream_key() {
            for col in input_stream_key {
                if !key_columns.contains(&col) {
                    key_columns.push(*col);
                }
            }
        } else {
            return None;
        }
        Some(key_columns)
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
