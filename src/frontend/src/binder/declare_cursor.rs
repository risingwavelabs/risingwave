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

use risingwave_sqlparser::ast::{Ident, ObjectName, Since};

use super::statement::RewriteExprsRecursive;
use crate::binder::BoundQuery;
use crate::expr::ExprRewriter;

#[derive(Debug, Clone)]
pub struct BoundDeclareCursor {
    pub cursor_name: Ident,
    // Currently we only support cursor with query
    pub query: Box<BoundQuery>, // reuse the BoundQuery struct
}

impl RewriteExprsRecursive for BoundDeclareCursor {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl ExprRewriter) {
        self.query.rewrite_exprs_recursive(rewriter);
    }
}

#[derive(Debug, Clone)]
pub struct BoundDeclareSubscriptionCursor {
    pub cursor_name: Ident,
    pub subscription_name: ObjectName,
    pub rw_timestamp: Since,
}
