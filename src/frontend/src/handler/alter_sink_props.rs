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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail;
use risingwave_pb::meta::alter_connector_props_request::{
    AlterConnectorPropsObject, AlterIcebergTablePropsObject, ObjectType,
};
use risingwave_sqlparser::ast::{ObjectName, SqlOption};

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::error::Result;
use crate::utils::resolve_connection_ref_and_secret_ref;
use crate::{Binder, WithOptions};

pub enum AlterSinkObject {
    Sink(ObjectName),
    // table_name + sink_name + source_name
    IcebergTable(ObjectName, ObjectName, ObjectName),
}

pub async fn handle_alter_sink_props(
    handler_args: HandlerArgs,
    alter_sink_object: AlterSinkObject,
    changed_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let (sink_id, object_type) = match alter_sink_object {
        AlterSinkObject::Sink(sink_name) => {
            let reader = session.env().catalog_reader().read_guard();
            let (schema_name, real_table_name) =
                Binder::resolve_schema_qualified_name(db_name, sink_name.clone())?;
            let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
            let (sink, schema_name) =
                reader.get_sink_by_name(db_name, schema_path, &real_table_name)?;

            if sink.target_table.is_some() {
                bail!("ALTER sink config is not for sink into table")
            }
            session.check_privilege_for_drop_alter(schema_name, &**sink)?;
            (
                sink.id.sink_id,
                ObjectType::AlterConnectorPropsObject(AlterConnectorPropsObject::Sink as i32),
            )
        }
        AlterSinkObject::IcebergTable(table_name, sink_name, source_name) => {
            let reader = session.env().catalog_reader().read_guard();
            let (schema_name, real_table_name) =
                Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
            let (_schema_name, real_sink_name) =
                Binder::resolve_schema_qualified_name(db_name, sink_name.clone())?;
            let (_schema_name, real_source_name) =
                Binder::resolve_schema_qualified_name(db_name, source_name.clone())?;
            let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
            let (sink, _schema_name) =
                reader.get_sink_by_name(db_name, schema_path, &real_sink_name)?;
            let (source, _schema_name) =
                reader.get_source_by_name(db_name, schema_path, &real_source_name)?;
            let (table, schema_name) =
                reader.get_table_by_name(db_name, schema_path, &real_table_name)?;
            if sink.target_table.is_some() {
                bail!("ALTER sink config is not for sink into table")
            }
            session.check_privilege_for_drop_alter(schema_name, &**sink)?;
            session.check_privilege_for_drop_alter(schema_name, &**source)?;
            session.check_privilege_for_drop_alter(schema_name, &**table)?;
            (
                sink.id.sink_id,
                ObjectType::AlterIcebergTablePropsObject(AlterIcebergTablePropsObject {
                    table_id: table.id.table_id as i32,
                    source_id: source.id as i32,
                    sink_id: sink.id.sink_id as i32,
                }),
            )
        }
    };

    let meta_client = session.env().meta_client();
    let (resolved_with_options, _, connector_conn_ref) = resolve_connection_ref_and_secret_ref(
        WithOptions::try_from(changed_props.as_ref() as &[SqlOption])?,
        &session,
        None,
    )?;
    let (changed_props, changed_secret_refs) = resolved_with_options.into_parts();
    if !changed_secret_refs.is_empty() || connector_conn_ref.is_some() {
        bail!("ALTER SINK does not support SECRET or CONNECTION now")
    }
    meta_client
        .alter_sink_props(
            sink_id,
            changed_props,
            changed_secret_refs,
            connector_conn_ref,
            object_type,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SINK))
}
