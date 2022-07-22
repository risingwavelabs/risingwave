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

use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::Result;
use risingwave_pb::user::grant_privilege::{Action as ProstAction, Object as ProstObject};
use risingwave_sqlparser::ast::{DropStatement, Statement, TableFactor};

use crate::binder::Binder;
use crate::session::SessionImpl;

pub(crate) fn resolve_privilege(
    session: &SessionImpl,
    stmt: &Statement,
) -> Result<(bool, ProstObject, ProstAction)> {
    let catalog_reader = session.env().catalog_reader();
    let reader = catalog_reader.read_guard();
    let action = match stmt {
        Statement::Insert { .. } => ProstAction::Insert,
        Statement::Delete { .. } => ProstAction::Delete,
        Statement::Update { .. } => ProstAction::Update,
        Statement::Drop(DropStatement { .. }) => ProstAction::Delete,
        _ => unreachable!(),
    };
    let is_owner;
    let object = match stmt {
        Statement::Insert { table_name, .. } | Statement::Delete { table_name, .. } => {
            let (schema_name, table_name) = Binder::resolve_table_name(table_name.clone())?;
            let table_catalog =
                reader.get_table_by_name(session.database(), &schema_name, &table_name)?;
            is_owner = table_catalog.owner == *session.user_name();
            ProstObject::TableId(table_catalog.id().table_id())
        }
        Statement::Update { table, .. } => {
            ensure!(table.joins.is_empty());
            let table_name = match &table.relation {
                TableFactor::Table { name, .. } => name.clone(),
                _ => unreachable!(),
            };
            let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;
            let table_catalog =
                reader.get_table_by_name(session.database(), &schema_name, &table_name)?;
            is_owner = table_catalog.owner == *session.user_name();
            ProstObject::TableId(table_catalog.id().table_id())
        }
        _ => unreachable!(),
    };
    Ok((is_owner, object, action))
}

pub(crate) fn check_privilege(
    session: &SessionImpl,
    object: &ProstObject,
    action: ProstAction,
) -> Result<()> {
    let user_reader = session.env().user_info_reader();
    let reader = user_reader.read_guard();

    if let Some(info) = reader.get_user_by_name(session.user_name()) {
        if info.is_supper {
            return Ok(());
        }
        let has_privilege = info.grant_privileges.iter().any(|privilege| {
            privilege.object.is_some()
                && privilege.object.as_ref().unwrap() == object
                && privilege
                    .action_with_opts
                    .iter()
                    .any(|ao| ao.action == action as i32)
        });
        if !has_privilege {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }
    } else {
        return Err(PermissionDenied("Session user is invalid".to_string()).into());
    }
    Ok(())
}
