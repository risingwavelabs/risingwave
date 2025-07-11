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

use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::Field;
use risingwave_sqlparser::ast::{DeclareCursor, Statement};

use super::declare_cursor::{BoundDeclareCursor, BoundDeclareSubscriptionCursor};
use super::delete::BoundDelete;
use super::fetch_cursor::BoundFetchCursor;
use super::update::BoundUpdate;
use crate::binder::create_view::BoundCreateView;
use crate::binder::{Binder, BoundInsert, BoundQuery};

// Bound COMPACT statement
#[derive(Debug, Clone)]
pub struct BoundCompact {
    pub table_name: String,
    pub schema_name: Option<String>,
}
use crate::error::Result;
use crate::expr::ExprRewriter;

#[derive(Debug, Clone)]
pub enum BoundStatement {
    Insert(Box<BoundInsert>),
    Delete(Box<BoundDelete>),
    Update(Box<BoundUpdate>),
    Query(Box<BoundQuery>),
    DeclareCursor(Box<BoundDeclareCursor>),
    DeclareSubscriptionCursor(Box<BoundDeclareSubscriptionCursor>),
    FetchCursor(Box<BoundFetchCursor>),
    CreateView(Box<BoundCreateView>),
    Compact(Box<BoundCompact>),
}

impl BoundStatement {
    pub fn output_fields(&self) -> Vec<Field> {
        match self {
            BoundStatement::Insert(i) => i
                .returning_schema
                .as_ref()
                .map_or(vec![], |s| s.fields().into()),
            BoundStatement::Delete(d) => d
                .returning_schema
                .as_ref()
                .map_or(vec![], |s| s.fields().into()),
            BoundStatement::Update(u) => u
                .returning_schema
                .as_ref()
                .map_or(vec![], |s| s.fields().into()),
            BoundStatement::Query(q) => q.schema().fields().into(),
            BoundStatement::DeclareCursor(_) => vec![],
            BoundStatement::DeclareSubscriptionCursor(_) => vec![],
            BoundStatement::FetchCursor(f) => f
                .returning_schema
                .as_ref()
                .map_or(vec![], |s| s.fields().into()),
            BoundStatement::CreateView(_) => vec![],
            BoundStatement::Compact(_) => vec![],
        }
    }
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

            Statement::DeclareCursor { stmt } => match stmt.declare_cursor {
                DeclareCursor::Query(body) => {
                    let query = self.bind_query(*body)?;
                    Ok(BoundStatement::DeclareCursor(
                        BoundDeclareCursor {
                            cursor_name: stmt.cursor_name,
                            query: query.into(),
                        }
                        .into(),
                    ))
                }
                DeclareCursor::Subscription(subscription_name, rw_timestamp) => {
                    Ok(BoundStatement::DeclareSubscriptionCursor(
                        BoundDeclareSubscriptionCursor {
                            cursor_name: stmt.cursor_name,
                            subscription_name,
                            rw_timestamp,
                        }
                        .into(),
                    ))
                }
            },

            // Note(eric): Can I just bind CreateView to Query??
            Statement::CreateView {
                or_replace,
                materialized,
                if_not_exists,
                name,
                columns,
                query,
                emit_mode,
                with_options,
            } => {
                let query = self.bind_query(*query)?;
                let create_view = BoundCreateView::new(
                    or_replace,
                    materialized,
                    if_not_exists,
                    name,
                    columns,
                    query,
                    emit_mode,
                    with_options,
                );
                Ok(BoundStatement::CreateView(create_view.into()))
            }

            Statement::Compact { table_name } => Ok(BoundStatement::Compact(
                self.bind_compact(table_name)?.into(),
            )),

            _ => bail_not_implemented!("unsupported statement {:?}", stmt),
        }
    }

    /// Bind COMPACT statement
    pub fn bind_compact(
        &mut self,
        table_name: risingwave_sqlparser::ast::ObjectName,
    ) -> Result<BoundCompact> {
        use crate::error::ErrorCode;

        // Resolve schema-qualified table name
        let (schema_name, table_name) =
            Self::resolve_schema_qualified_name(&self.db_name, table_name)?;

        // Check if table exists and is an iceberg table
        let schema_path = self.bind_schema_path(schema_name.as_deref());
        let (table_catalog, _) =
            self.catalog
                .get_created_table_by_name(&self.db_name, schema_path, &table_name)?;

        // Validate this is an iceberg table
        if table_catalog.engine() != risingwave_common::catalog::Engine::Iceberg {
            return Err(ErrorCode::InvalidInputSyntax(
                format!("COMPACT can only be used on Iceberg engine tables, but table '{}' uses {:?} engine",
                        table_name, table_catalog.engine())
            ).into());
        }

        // Check permissions - user needs SELECT privilege to compact a table
        use risingwave_common::acl::AclMode;
        use risingwave_pb::user::grant_privilege::PbObject;

        use crate::handler::privilege::ObjectCheckItem;

        self.check_privilege(
            ObjectCheckItem::new(
                table_catalog.owner,
                AclMode::Select,
                table_name.clone(),
                PbObject::TableId(table_catalog.id.table_id),
            ),
            table_catalog.database_id,
        )?;

        Ok(BoundCompact {
            table_name,
            schema_name,
        })
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
            BoundStatement::DeclareCursor(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundStatement::FetchCursor(_) => {}
            BoundStatement::DeclareSubscriptionCursor(_) => {}
            BoundStatement::CreateView(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundStatement::Compact(_) => {}
        }
    }
}
