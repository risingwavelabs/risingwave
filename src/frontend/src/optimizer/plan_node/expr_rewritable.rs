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

use std::ops::Deref;

use super::*;
use crate::expr::ExprRewriter;

/// Rewrites expressions in a `PlanRef`. Due to `Share` operator,
/// the `ExprRewriter` needs to be idempotent i.e. applying it more than once
/// to the same `ExprImpl` will be a noop on subsequent applications.
/// `rewrite_exprs` should only return a plan with the given node modified.
/// To rewrite recursively, call `rewrite_exprs_recursive` on [`RewriteExprsRecursive`].
pub trait ExprRewritable {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
        unimplemented!()
    }
}

impl ExprRewritable for PlanRef {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        if self.deref().has_rewritable_expr() {
            self.deref().rewrite_exprs(r)
        } else {
            self.clone()
        }
    }
}
