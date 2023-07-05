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

use crate::binder::statement::RewriteExprsRecursive;
use crate::binder::{Relation, ShareId};

/// Share a relation during binding and planning.
/// It could be used to share a CTE, a source, a view and so on.
#[derive(Debug, Clone)]
pub struct BoundShare {
    pub(crate) share_id: ShareId,
    pub(crate) input: Relation,
}

impl RewriteExprsRecursive for BoundShare {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.input.rewrite_exprs_recursive(rewriter);
    }
}
