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

//! Handle creation of logical (non-materialized) views.

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::PbView;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, Statement};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::handler::HandlerArgs;
use crate::optimizer::OptimizerContext;
use crate::session::CheckRelationError;

pub async fn handle_create_view(
    handler_args: HandlerArgs,
    if_not_exists: bool,
    name: ObjectName,
    columns: Vec<Ident>,
    query: Query,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = session.database();
    let (schema_name, view_name) = Binder::resolve_schema_qualified_name(db_name, name.clone())?;

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let properties = handler_args.with_options.clone();

    match session.check_relation_name_duplicated(name.clone()) {
        Err(CheckRelationError::Catalog(CatalogError::Duplicated(_, name))) if if_not_exists => {
            return Ok(PgResponse::builder(StatementType::CREATE_VIEW)
                .notice(format!("relation \"{}\" already exists, skipping", name))
                .into());
        }
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    };

    // plan the query to validate it and resolve dependencies
    let (dependent_relations, schema) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let super::query::BatchQueryPlanResult {
            schema,
            dependent_relations,
            ..
        } = super::query::gen_batch_plan_by_statement(
            &session,
            context.into(),
            Statement::Query(Box::new(query.clone())),
        )?;

        (dependent_relations, schema)
    };

    let columns = if columns.is_empty() {
        schema.fields().to_vec()
    } else {
        if columns.len() != schema.fields().len() {
            return Err(risingwave_common::error::ErrorCode::InternalError(
                "view has different number of columns than the query's columns".to_string(),
            )
            .into());
        }
        schema
            .fields()
            .iter()
            .zip_eq_fast(columns)
            .map(|(f, c)| {
                let mut field = f.clone();
                field.name = c.real_value();
                field
            })
            .collect()
    };

    let view = PbView {
        id: 0,
        schema_id,
        database_id,
        name: view_name,
        properties: properties.inner().clone().into_iter().collect(),
        owner: session.user_id(),
        dependent_relations: dependent_relations
            .into_iter()
            .map(|t| t.table_id)
            .collect_vec(),
        sql: format!("{}", query),
        columns: columns.into_iter().map(|f| f.to_prost()).collect(),
        description: None,
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_view(view).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_VIEW))
}
