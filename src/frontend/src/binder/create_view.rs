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

use risingwave_sqlparser::ast::{EmitMode, Ident, ObjectName, SqlOption};

use crate::binder::BoundQuery;
use crate::binder::statement::RewriteExprsRecursive;
use crate::expr::ExprRewriter;

/// Represents a bounded `CREATE MATERIALIZED VIEW` statement.
#[derive(Debug, Clone)]
pub struct BoundCreateView {
    pub or_replace: bool,
    pub materialized: bool, // always true currently
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub query: Box<BoundQuery>, // reuse the BoundQuery struct
    pub emit_mode: Option<EmitMode>,
    pub with_options: Vec<SqlOption>,
}

impl BoundCreateView {
    pub fn new(
        or_replace: bool,
        materialized: bool,
        if_not_exists: bool,
        name: ObjectName,
        columns: Vec<Ident>,
        query: BoundQuery,
        emit_mode: Option<EmitMode>,
        with_options: Vec<SqlOption>,
    ) -> Self {
        Self {
            or_replace,
            materialized,
            if_not_exists,
            name,
            columns,
            query: Box::new(query),
            emit_mode,
            with_options,
        }
    }
}

impl RewriteExprsRecursive for BoundCreateView {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl ExprRewriter) {
        self.query.rewrite_exprs_recursive(rewriter);
    }
}
