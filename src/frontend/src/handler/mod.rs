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

use std::sync::Arc;

use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{DropStatement, ObjectName, ObjectType, Statement};

use crate::session::{OptimizerContext, SessionImpl};

pub mod create_mv;
mod create_source;
pub mod create_table;
mod describe;
pub mod drop_mv;
pub mod drop_table;
mod explain;
mod flush;
#[allow(dead_code)]
pub mod query;
pub mod query_single;
mod set;
mod show;
pub mod util;

pub(super) async fn handle(session: Arc<SessionImpl>, stmt: Statement) -> Result<PgResponse> {
    let context = OptimizerContext::new(session.clone());
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(context, *statement, verbose),
        Statement::CreateSource {
            is_materialized,
            stmt,
        } => create_source::handle_create_source(context, is_materialized, stmt).await,
        Statement::CreateTable { name, columns, .. } => {
            create_table::handle_create_table(context, name, columns).await
        }
        Statement::Describe { name } => describe::handle_describe(context, name).await,
        // TODO: support complex sql for `show columns from <table>`
        Statement::ShowColumn { name } => describe::handle_describe(context, name).await,
        Statement::ShowCommand(show_object) => {
            show::handle_show_command(context, show_object).await
        }
        Statement::Drop(DropStatement {
            object_type, name, ..
        }) => {
            let name = ObjectName(vec![name]);
            match object_type {
                ObjectType::Table => drop_table::handle_drop_table(context, name).await,
                ObjectType::MaterializedView => drop_mv::handle_drop_mv(context, name).await,
                ObjectType::MaterializedSource => {
                    // FIXME: We currently treat MATERIALIZE SOURCE as an alias TABLE, while
                    // this assumption is not correct. DROP MATERIALIZE SOURCE should only drops
                    // materialized sources.
                    drop_table::handle_drop_table(context, name).await
                }
                _ => Err(ErrorCode::InvalidInputSyntax(format!(
                    "DROP {} is unsupported",
                    object_type
                ))
                .into()),
            }
        }
        Statement::Query(_) => {
            if context.session_ctx.env().query_manager().dist_query() {
                query::handle_query(context, stmt).await
            } else {
                query_single::handle_query_single(context, stmt).await
            }
        }
        Statement::Insert { .. } | Statement::Delete { .. } => {
            query_single::handle_query_single(context, stmt).await
        }
        Statement::CreateView {
            materialized: true,
            or_replace: false,
            name,
            query,
            ..
        } => create_mv::handle_create_mv(context, name, query).await,
        Statement::Flush => flush::handle_flush(context).await,
        Statement::SetVariable {
            local: _,
            variable,
            value,
        } => set::handle_set(context, variable, value),
        _ => {
            Err(ErrorCode::NotImplemented(format!("Unhandled ast: {:?}", stmt), None.into()).into())
        }
    }
}
