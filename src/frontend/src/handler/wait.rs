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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::id::JobId;
use risingwave_sqlparser::ast::WaitTarget;

use super::RwPgResponse;
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;

pub(super) async fn handle_wait(
    handler_args: HandlerArgs,
    target: WaitTarget,
) -> Result<RwPgResponse> {
    do_wait(&handler_args.session, target).await?;
    Ok(PgResponse::empty_result(StatementType::WAIT))
}

pub(crate) async fn do_wait(session: &SessionImpl, target: WaitTarget) -> Result<()> {
    let catalog_writer = session.catalog_writer()?;
    let job_id = resolve_wait_job_id(session, target)?;
    catalog_writer.wait(job_id).await?;
    Ok(())
}

fn resolve_wait_job_id(session: &SessionImpl, target: WaitTarget) -> Result<Option<JobId>> {
    let db_name = &session.database();
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let reader = session.env().catalog_reader().read_guard();

    match target {
        WaitTarget::All => Ok(None),
        WaitTarget::Table(name) => {
            let (schema_name, real_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;
            let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
            let (table, schema_name) =
                reader.get_any_table_by_name(db_name, schema_path, &real_name)?;
            if !table.is_user_table() {
                return Err(table.bad_drop_error());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            Ok(Some(table.id.as_job_id()))
        }
        WaitTarget::MaterializedView(name) => {
            let (schema_name, real_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;
            let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
            let (table, schema_name) =
                reader.get_any_table_by_name(db_name, schema_path, &real_name)?;
            if !table.is_mview() {
                return Err(table.bad_drop_error());
            }
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            Ok(Some(table.id.as_job_id()))
        }
        WaitTarget::Sink(name) => {
            let (schema_name, real_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;
            let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
            let (sink, schema_name) =
                reader.get_any_sink_by_name(db_name, schema_path, &real_name)?;
            session.check_privilege_for_drop_alter(schema_name, &**sink)?;
            Ok(Some(sink.id.as_job_id()))
        }
        WaitTarget::Index(name) => {
            let (schema_name, real_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;
            let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
            let (index, schema_name) =
                reader.get_any_index_by_name(db_name, schema_path, &real_name)?;
            session.check_privilege_for_drop_alter(schema_name, &**index)?;
            Ok(Some(index.id.as_job_id()))
        }
    }
}
