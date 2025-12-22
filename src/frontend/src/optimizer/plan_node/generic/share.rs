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

use std::hash::Hash;
use std::sync::{Arc, Mutex};

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone)]
pub struct Share<PlanRef> {
    pub input: Arc<Mutex<PlanRef>>,
}

impl<P: PartialEq> PartialEq for Share<P> {
    fn eq(&self, other: &Self) -> bool {
        *self.input.lock().unwrap() == *other.input.lock().unwrap()
    }
}

impl<P: Eq> Eq for Share<P> {}

impl<P: Hash> Hash for Share<P> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.lock().unwrap().hash(state);
    }
}

impl<PlanRef: GenericPlanRef> Share<PlanRef> {
    pub fn new(input: PlanRef) -> Self {
        Self {
            input: Arc::new(Mutex::new(input)),
        }
    }

    pub fn replace_input(&self, plan: PlanRef) {
        *self.input.lock().unwrap() = plan;
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Share<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.lock().unwrap().schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.input.lock().unwrap().stream_key()?.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.lock().unwrap().ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.lock().unwrap().functional_dependency().clone()
    }
}
