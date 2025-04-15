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

use std::sync::Arc;

use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_common::session_config::RuntimeParameters;
use risingwave_pb::ddl_service::alter_swap_rename_request;
use risingwave_pb::ddl_service::alter_swap_rename_request::ObjectNameSwapPair;
use risingwave_sqlparser::ast::ObjectName;

use crate::Binder;
use crate::catalog::CatalogError;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::session::SessionImpl;
use crate::user::UserId;

/// Check if the session user has the privilege to swap and rename the objects.
fn check_swap_rename_privilege(
    session_impl: &Arc<SessionImpl>,
    src_owner: UserId,
    target_owner: UserId,
) -> Result<()> {
    if !session_impl.is_super_user()
        && (src_owner != session_impl.user_id() || target_owner != session_impl.user_id())
    {
        return Err(ErrorCode::PermissionDenied(format!(
            "{} is not super user and not the owner of the objects.",
            session_impl.user_name()
        ))
        .into());
    }
    Ok(())
}

pub async fn handle_swap_rename(
    handler_args: HandlerArgs,
    source_object: ObjectName,
    target_object: ObjectName,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (src_schema_name, src_obj_name) =
        Binder::resolve_schema_qualified_name(db_name, source_object)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
    let user_name = &session.user_name();
    let src_schema_path = SchemaPath::new(src_schema_name.as_deref(), &search_path, user_name);
    let (target_schema_name, target_obj_name) =
        Binder::resolve_schema_qualified_name(db_name, target_object)?;
    let target_schema_path =
        SchemaPath::new(target_schema_name.as_deref(), &search_path, user_name);

    let obj = match stmt_type {
        StatementType::ALTER_SCHEMA => {
            // TODO: support it until resolves https://github.com/risingwavelabs/risingwave/issues/19028
            bail_not_implemented!("ALTER SCHEMA SWAP WITH is not supported yet");
        }
        StatementType::ALTER_TABLE | StatementType::ALTER_MATERIALIZED_VIEW => {
            let catalog_reader = session.env().catalog_reader().read_guard();
            let (src_table, _) = catalog_reader.get_created_table_by_name(
                db_name,
                src_schema_path,
                &src_obj_name,
            )?;
            let (target_table, _) = catalog_reader.get_created_table_by_name(
                db_name,
                target_schema_path,
                &target_obj_name,
            )?;

            if src_table.table_type != target_table.table_type {
                return Err(ErrorCode::PermissionDenied(format!(
                    "cannot swap between {} and {}: type mismatch",
                    src_obj_name, target_obj_name
                ))
                .into());
            }
            if stmt_type == StatementType::ALTER_TABLE && !src_table.is_user_table() {
                return Err(CatalogError::NotFound("table", src_obj_name.clone()).into());
            } else if stmt_type == StatementType::ALTER_MATERIALIZED_VIEW && !src_table.is_mview() {
                return Err(
                    CatalogError::NotFound("materialized view", src_obj_name.clone()).into(),
                );
            }

            check_swap_rename_privilege(&session, src_table.owner, target_table.owner)?;

            alter_swap_rename_request::Object::Table(ObjectNameSwapPair {
                src_object_id: src_table.id.table_id,
                dst_object_id: target_table.id.table_id,
            })
        }
        StatementType::ALTER_VIEW => {
            let catalog_reader = session.env().catalog_reader().read_guard();
            let (src_view, _) =
                catalog_reader.get_view_by_name(db_name, src_schema_path, &src_obj_name)?;
            let (target_view, _) =
                catalog_reader.get_view_by_name(db_name, target_schema_path, &target_obj_name)?;
            check_swap_rename_privilege(&session, src_view.owner, target_view.owner)?;

            alter_swap_rename_request::Object::View(ObjectNameSwapPair {
                src_object_id: src_view.id,
                dst_object_id: target_view.id,
            })
        }
        StatementType::ALTER_SOURCE => {
            let catalog_reader = session.env().catalog_reader().read_guard();
            let (src_source, _) =
                catalog_reader.get_source_by_name(db_name, src_schema_path, &src_obj_name)?;
            let (target_source, _) =
                catalog_reader.get_source_by_name(db_name, target_schema_path, &target_obj_name)?;
            check_swap_rename_privilege(&session, src_source.owner, target_source.owner)?;

            alter_swap_rename_request::Object::Source(ObjectNameSwapPair {
                src_object_id: src_source.id,
                dst_object_id: target_source.id,
            })
        }
        StatementType::ALTER_SINK => {
            let catalog_reader = session.env().catalog_reader().read_guard();
            let (src_sink, _) =
                catalog_reader.get_sink_by_name(db_name, src_schema_path, &src_obj_name)?;
            let (target_sink, _) =
                catalog_reader.get_sink_by_name(db_name, target_schema_path, &target_obj_name)?;
            check_swap_rename_privilege(
                &session,
                src_sink.owner.user_id,
                target_sink.owner.user_id,
            )?;

            alter_swap_rename_request::Object::Sink(ObjectNameSwapPair {
                src_object_id: src_sink.id.sink_id,
                dst_object_id: target_sink.id.sink_id,
            })
        }
        StatementType::ALTER_SUBSCRIPTION => {
            let catalog_reader = session.env().catalog_reader().read_guard();
            let (src_subscription, _) =
                catalog_reader.get_subscription_by_name(db_name, src_schema_path, &src_obj_name)?;
            let (target_subscription, _) = catalog_reader.get_subscription_by_name(
                db_name,
                target_schema_path,
                &target_obj_name,
            )?;
            check_swap_rename_privilege(
                &session,
                src_subscription.owner.user_id,
                target_subscription.owner.user_id,
            )?;

            alter_swap_rename_request::Object::Subscription(ObjectNameSwapPair {
                src_object_id: src_subscription.id.subscription_id,
                dst_object_id: target_subscription.id.subscription_id,
            })
        }
        _ => {
            unreachable!("handle_swap_rename: unsupported statement type")
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.alter_swap_rename(obj).await?;

    Ok(RwPgResponse::empty_result(stmt_type))
}
