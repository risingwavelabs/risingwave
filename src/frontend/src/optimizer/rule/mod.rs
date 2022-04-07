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

//! Define all [`Rule`]

use super::PlanRef;
use crate::expr::{CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};

/// A one-to-one transform for the [`PlanNode`](super::plan_node::PlanNode), every [`Rule`] should
/// downcast and check if the node matches the rule.
pub trait Rule: Send + Sync + 'static {
    /// return err(()) if not match
    fn apply(&self, plan: PlanRef) -> Option<PlanRef>;
}

pub(super) type BoxedRule = Box<dyn Rule>;

mod project_join;
pub use project_join::*;
mod filter_join;
pub use filter_join::*;
mod filter_project;
pub use filter_project::*;
mod filter_agg;
pub use filter_agg::*;
mod project_elim;
pub use project_elim::*;
mod project_merge;
pub use project_merge::*;
mod apply_project;
pub use apply_project::*;
mod apply_filter;
pub use apply_filter::*;
mod apply_scan;
pub use apply_scan::*;

struct LiftCorrelatedInputRef {}

impl ExprRewriter for LiftCorrelatedInputRef {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        match correlated_input_ref.depth() {
            0 => unreachable!(),
            1 => InputRef::new(
                correlated_input_ref.index(),
                correlated_input_ref.return_type(),
            )
            .into(),
            depth => CorrelatedInputRef::new(
                correlated_input_ref.index(),
                correlated_input_ref.return_type(),
                depth - 1,
            )
            .into(),
        }
    }
}
