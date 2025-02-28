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

use itertools::Itertools;
use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef, impl_distill_unit_from_fields};
use crate::binder::ShareId;
use crate::optimizer::property::FunctionalDependencySet;
use crate::{OptimizerContextRef, optimizer};

#[derive(Clone, Debug)]
pub struct CteRef<PlanRef> {
    share_id: ShareId,
    base: PlanRef,
}

impl<PlanRef> PartialEq for CteRef<PlanRef> {
    fn eq(&self, other: &Self) -> bool {
        self.share_id == other.share_id
    }
}

impl<PlanRef> Eq for CteRef<PlanRef> {}

impl<PlanRef> Hash for CteRef<PlanRef> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.share_id.hash(state);
    }
}

impl<PlanRef> CteRef<PlanRef> {
    pub fn new(share_id: ShareId, base: PlanRef) -> Self {
        Self { share_id, base }
    }
}

impl<PlanRef: GenericPlanRef> CteRef<PlanRef> {
    pub fn get_cte_ref(&self) -> Option<optimizer::plan_node::PlanRef> {
        self.ctx().get_rcte_cache_plan(&self.share_id)
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for CteRef<PlanRef> {
    fn schema(&self) -> Schema {
        if let Some(plan_ref) = self.get_cte_ref() {
            plan_ref.schema().clone()
        } else {
            self.base.schema().clone()
        }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        if let Some(plan_ref) = self.get_cte_ref() {
            plan_ref
                .stream_key()
                .map(|s| s.iter().map(|i| i.to_owned()).collect_vec())
        } else {
            self.base
                .stream_key()
                .map(|s| s.iter().map(|i| i.to_owned()).collect_vec())
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        // it does not matter where the context is coming from,
        // since we are only getting a reference.
        self.base.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        if let Some(plan_ref) = self.get_cte_ref() {
            plan_ref.functional_dependency().clone()
        } else {
            self.base.functional_dependency().clone()
        }
    }
}

impl<PlanRef: GenericPlanRef> CteRef<PlanRef> {
    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        vec![("share_id", Pretty::debug(&self.share_id))]
    }
}

impl_distill_unit_from_fields! {CteRef, GenericPlanRef}
