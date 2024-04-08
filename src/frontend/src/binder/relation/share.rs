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

use either::Either;

use super::{BoundSubquery, Relation};
use crate::binder::bind_context::RecursiveUnion;
use crate::binder::statement::RewriteExprsRecursive;
use crate::binder::{BoundQuery, ShareId};

/// Share a relation during binding and planning.
/// It could be used to share a (recursive) CTE, a source, a view and so on.
#[derive(Debug, Clone)]
pub struct BoundShare {
    pub(crate) share_id: ShareId,
    pub(crate) input: Either<BoundQuery, RecursiveUnion>,
}

impl RewriteExprsRecursive for BoundShare {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        let rewrite = match self.input.clone() {
            Either::Left(mut q) => {
                q.rewrite_exprs_recursive(rewriter);
                Either::Left(q)
            }
            Either::Right(mut r) => {
                r.rewrite_exprs_recursive(rewriter);
                Either::Right(r)
            }
        };
        self.input = rewrite;
    }
}

/// from inner `BoundQuery` to `Relation::Subquery`
impl From<BoundShare> for Relation {
    fn from(value: BoundShare) -> Self {
        let Either::Left(q) = value.input else {
            // leave it intact
            return Self::Share(Box::new(value));
        };
        Self::Subquery(Box::new(BoundSubquery {
            query: q,
            lateral: false,
        }))
    }
}
