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

use std::cell::RefCell;
use std::hash::Hash;

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Share<PlanRef> {
    pub input: RefCell<PlanRef>,
}

impl<P: Hash> Hash for Share<P> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.borrow().hash(state);
    }
}

impl<PlanRef: GenericPlanRef> Share<PlanRef> {
    pub fn replace_input(&self, plan: PlanRef) {
        *self.input.borrow_mut() = plan;
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Share<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.borrow().schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.borrow().logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.borrow().ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.borrow().functional_dependency().clone()
    }
}
