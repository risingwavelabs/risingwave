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

//! Handle creation of logical (non-materialized) views.

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::View as ProstView;
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_sqlparser::ast::{Ident, ObjectName, Query};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};

pub async fn handle_create_view(
    context: OptimizerContext,
    name: ObjectName,
    columns: Vec<Ident>,
    query: Query,
) -> Result<RwPgResponse> {
    let session = context.session_ctx.clone();

    session.check_relation_name_duplicated(name.clone())?;

    let db_name = session.database();
    let (schema_name, view_name) = Binder::resolve_schema_qualified_name(db_name, name.clone())?;

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let properties = context.with_options.clone();
    let view = ProstView {
        id: 0,
        schema_id,
        database_id,
        name: view_name,
        properties: properties.inner().clone(),
        owner: session.user_id(),
        dependent_relations: vec![],
        sql: format!("{}", query),
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_view(view).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_VIEW))
}
