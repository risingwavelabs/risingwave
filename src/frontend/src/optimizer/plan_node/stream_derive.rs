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

use risingwave_common::catalog::Schema;

use super::generic::GenericPlanNode;
use super::stream::*;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{Distribution, FunctionalDependencySet};
use crate::utils::ColIndexMappingRewriteExt;

impl GenericPlanNode for DynamicFilter {
    fn schema(&self) -> Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        todo!("new plan node derivation")
    }
}
impl StreamPlanNode for DynamicFilter {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for Exchange {
    fn schema(&self) -> Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        todo!("new plan node derivation")
    }
}

impl StreamPlanNode for Exchange {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for DeltaJoin {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for DeltaJoin {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for Expand {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for Expand {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for Filter {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for Filter {
    fn distribution(&self) -> Distribution {
        self.core.input.distribution().clone()
    }

    fn append_only(&self) -> bool {
        self.core.input.append_only()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for SimpleAgg {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for SimpleAgg {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for GroupTopN {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for GroupTopN {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for HashAgg {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for HashAgg {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for HashJoin {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for HashJoin {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for HopWindow {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for HopWindow {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for IndexScan {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for IndexScan {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for LocalSimpleAgg {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for LocalSimpleAgg {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for Materialize {
    fn schema(&self) -> Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        todo!("new plan node derivation")
    }
}

impl StreamPlanNode for Materialize {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for ProjectSet {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for ProjectSet {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for Project {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for Project {
    fn distribution(&self) -> Distribution {
        self.core
            .i2o_col_mapping()
            .rewrite_provided_distribution(self.core.input.distribution())
    }

    fn append_only(&self) -> bool {
        self.core.input.append_only()
    }

    fn emit_on_window_close(&self) -> bool {
        self.core.input.emit_on_window_close()
    }
}

impl GenericPlanNode for Sink {
    fn schema(&self) -> Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        todo!("new plan node derivation")
    }
}

impl StreamPlanNode for Sink {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for Source {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for Source {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for TableScan {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for TableScan {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}

impl GenericPlanNode for TopN {
    fn schema(&self) -> Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.core.functional_dependency()
    }
}

impl StreamPlanNode for TopN {
    fn distribution(&self) -> Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }

    fn emit_on_window_close(&self) -> bool {
        todo!()
    }
}
