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
use crate::binder::BoundQuery;

/// a *bound* recursive union representation.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BoundRecursiveUnion {
    /// the *bound* base case
    pub(crate) base: BoundQuery,
    /// the *bound* recursive case
    pub(crate) recursive: BoundQuery,
}

impl RewriteExprsRecursive for BoundRecursiveUnion {
    fn rewrite_exprs_recursive(&mut self, _rewriter: &mut impl crate::expr::ExprRewriter) {}
}
