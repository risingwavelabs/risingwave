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

use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use super::delete::BoundDelete;
use super::update::BoundUpdate;
use crate::binder::{Binder, BoundInsert, BoundQuery};
use crate::expr::ExprRewriter;

#[derive(Debug)]
pub enum BoundStatement {
    Insert(Box<BoundInsert>),
    Delete(Box<BoundDelete>),
    Update(Box<BoundUpdate>),
    Query(Box<BoundQuery>),
}

impl Binder {
    pub(super) fn bind_statement(&mut self, stmt: Statement) -> Result<BoundStatement> {
        match stmt {
            Statement::Insert {
                table_name,
                columns,
                source,
                returning,
            } => Ok(BoundStatement::Insert(
                self.bind_insert(table_name, columns, *source, returning)?
                    .into(),
            )),

            Statement::Delete {
                table_name,
                selection,
                returning,
            } => Ok(BoundStatement::Delete(
                self.bind_delete(table_name, selection, returning)?.into(),
            )),

            Statement::Update {
                table_name,
                assignments,
                selection,
                returning,
            } => Ok(BoundStatement::Update(
                self.bind_update(table_name, assignments, selection, returning)?
                    .into(),
            )),

            Statement::Query(q) => Ok(BoundStatement::Query(self.bind_query(*q)?.into())),

            _ => Err(ErrorCode::NotImplemented(
                format!("unsupported statement {:?}", stmt),
                None.into(),
            )
            .into()),
        }
    }
}

pub(crate) trait RewriteExprsRecursive {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl ExprRewriter);
}

impl RewriteExprsRecursive for BoundStatement {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl ExprRewriter) {
        match self {
            BoundStatement::Insert(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundStatement::Delete(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundStatement::Update(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundStatement::Query(inner) => inner.rewrite_exprs_recursive(rewriter),
        }
    }
}
