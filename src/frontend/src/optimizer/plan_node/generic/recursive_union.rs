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

use pretty_xmlish::StrAssocArr;
use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef, impl_distill_unit_from_fields};
use crate::OptimizerContextRef;
use crate::binder::ShareId;
use crate::optimizer::property::FunctionalDependencySet;

/// `RecursiveUnion` returns the union of the rows of its inputs.
/// note: if `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecursiveUnion<PlanRef> {
    pub id: ShareId,
    pub base: PlanRef,
    pub recursive: PlanRef,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for RecursiveUnion<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.recursive.functional_dependency().clone()
    }

    fn schema(&self) -> Schema {
        self.recursive.schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let fields_len = self.base.schema().len();
        let base = self.base.stream_key();
        if let Some(base) = base {
            let mut base = base.to_vec();
            base.push(fields_len);
            Some(base)
        } else {
            None
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.recursive.ctx()
    }
}

impl<PlanRef: GenericPlanRef> RecursiveUnion<PlanRef> {
    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        vec![]
    }
}

impl_distill_unit_from_fields!(RecursiveUnion, GenericPlanRef);
