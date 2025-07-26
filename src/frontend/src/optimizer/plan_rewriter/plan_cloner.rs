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

use std::marker::PhantomData;

use crate::PlanRef;
use crate::optimizer::plan_node::ConventionMarker;
use crate::optimizer::plan_rewriter::PlanRewriter;

#[derive(Debug, Clone)]
pub struct PlanCloner<C: ConventionMarker>(PhantomData<C>);

impl<C: ConventionMarker> PlanCloner<C> {
    pub fn clone_whole_plan(plan: PlanRef) -> PlanRef {
        let mut plan_cloner = Self(PhantomData);
        plan.rewrite_with(&mut plan_cloner)
    }
}

impl<C: ConventionMarker> PlanRewriter<C> for PlanCloner<C> {
    fn rewrite_with_inputs(&mut self, plan: &PlanRef, inputs: Vec<PlanRef>) -> PlanRef {
        plan.clone_root_with_inputs::<C>(&inputs)
    }
}
