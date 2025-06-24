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

use std::collections::HashSet;
use std::sync::Arc;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ConflictBehavior, FunctionId};
use risingwave_sqlparser::ast::{EmitMode, Ident, ObjectName, Query, Statement};

use super::{HandlerArgs, RwPgResponse};
use crate::TableCatalog;
use crate::binder::{Binder, BoundQuery};
use crate::catalog::TableId;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};
use crate::handler::create_mv;
use crate::session::SessionImpl;

/// Fetch materialized view catalog for alter operations, similar to `fetch_table_catalog_for_alter`
/// but checks for `TableType::MaterializedView`
pub fn fetch_mv_catalog_for_alter(
    session: &SessionImpl,
    mv_name: &ObjectName,
) -> Result<Arc<TableCatalog>> {
    let db_name = &session.database();
    let (schema_name, real_mv_name) =
        Binder::resolve_schema_qualified_name(db_name, mv_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_created_table_by_name(db_name, schema_path, &real_mv_name)?;

        match table.table_type() {
            TableType::MaterializedView => {}

            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{mv_name}\" is not a materialized view or cannot be altered"
            )))?,
        }

        session.check_privilege_for_drop_alter(schema_name, &**table)?;

        table.clone()
    };

    Ok(original_catalog)
}

pub async fn handle_alter_mv(
    handler_args: HandlerArgs,
    name: ObjectName,
    new_query: Box<Query>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let original_catalog = fetch_mv_catalog_for_alter(session.as_ref(), &name)?;

    // Retrieve the original MV definition and parse it to AST
    let original_definition = original_catalog.create_sql_ast()?;

    // Extract unchanged parts from the original definition
    let (columns, with_options, emit_mode) = match &original_definition {
        Statement::CreateView {
            columns,
            with_options,
            emit_mode,
            ..
        } => (columns.clone(), with_options.clone(), emit_mode.clone()),
        _ => {
            return Err(ErrorCode::InternalError(format!(
                "Expected CREATE MATERIALIZED VIEW statement, got: {:?}",
                original_definition
            ))
            .into());
        }
    };

    // Create a new CREATE MATERIALIZED VIEW statement with the new query
    let new_definition = Statement::CreateView {
        or_replace: false,
        materialized: true,
        if_not_exists: false,
        name: name.clone(),
        columns: columns.clone(),
        query: new_query.clone(),
        with_options,
        emit_mode: emit_mode.clone(),
    };
    let handler_args = HandlerArgs::new(session.clone(), &new_definition, Arc::from(""))?;

    handle_alter_mv_as_if(handler_args, name, *new_query, columns, emit_mode).await
}

pub async fn handle_alter_mv_as_if(
    handler_args: HandlerArgs,
    name: ObjectName,
    query: Query,
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
) -> Result<RwPgResponse> {
    let (dependent_relations, dependent_udfs, bound_query) = {
        let mut binder = Binder::new_for_stream(handler_args.session.as_ref());
        let bound_query = binder.bind_query(query)?;
        (
            binder.included_relations().clone(),
            binder.included_udfs().clone(),
            bound_query,
        )
    };

    handle_alter_mv_bound(
        handler_args,
        name,
        bound_query,
        dependent_relations,
        dependent_udfs,
        columns,
        emit_mode,
    )
    .await
}

async fn handle_alter_mv_bound(
    handler_args: HandlerArgs,
    name: ObjectName,
    query: BoundQuery,
    dependent_relations: HashSet<TableId>,
    dependent_udfs: HashSet<FunctionId>, // TODO(rc): merge with `dependent_relations`
    columns: Vec<Ident>,
    emit_mode: Option<EmitMode>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    // TODO(alter-mv): use `ColumnIdGenerator` to generate IDs for MV columns, in order to
    // support schema changes.
    let (table, graph, _dependencies, _resource_group) = {
        create_mv::gen_create_mv_graph(
            handler_args,
            name,
            query,
            dependent_relations,
            dependent_udfs,
            columns,
            emit_mode,
            &session,
        )
        .await?
    };

    let session = session.clone();
    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .replace_materialized_view(table.to_prost(), graph)
        .await?;

    Ok(PgResponse::empty_result(
        StatementType::ALTER_MATERIALIZED_VIEW,
    ))
}
