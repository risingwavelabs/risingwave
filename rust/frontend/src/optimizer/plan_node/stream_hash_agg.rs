// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::fmt;

use risingwave_common::catalog::Schema;

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamHashAgg {
    pub base: PlanBase,
    logical: LogicalAgg,
}

impl StreamHashAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = PlanBase::new_stream(ctx, logical.schema().clone(), Distribution::any().clone());
        StreamHashAgg { logical, base }
    }
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
    pub fn group_keys(&self) -> &[usize] {
        self.logical.group_keys()
    }
}

impl fmt::Display for StreamHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamHashAgg")
            .field("group_keys", &self.group_keys())
            .field("aggs", &self.agg_calls())
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamHashAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { StreamHashAgg }

impl WithSchema for StreamHashAgg {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamHashAgg {}
