// Copyright 2024 RisingWave Labs
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

use crate::binder::statement::RewriteExprsRecursive;
use crate::binder::{BoundSetExpr, ShareId};

/// A CTE reference, currently only used in the back reference of recursive CTE.
/// For the non-recursive one, see [`BoundShare`](super::BoundShare).
#[derive(Debug, Clone)]
pub struct BoundBackCteRef {
    pub(crate) share_id: ShareId,
    pub(crate) base: BoundSetExpr,
}

impl RewriteExprsRecursive for BoundBackCteRef {
    fn rewrite_exprs_recursive(&mut self, _rewriter: &mut impl crate::expr::ExprRewriter) {}
}
