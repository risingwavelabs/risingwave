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

use itertools::Itertools;
use risingwave_common::acl;
use risingwave_common::acl::{AclMode, AclModeSet};
use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;
use risingwave_pb::user::PbGrantPrivilege;
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, PbAction, PbObject};
use risingwave_sqlparser::ast::{Action, GrantObjects, Privileges};

use crate::error::{ErrorCode, Result};

pub fn check_privilege_type(privilege: &Privileges, objects: &GrantObjects) -> Result<()> {
    match privilege {
        Privileges::All { .. } => Ok(()),
        Privileges::Actions(actions) => {
            let acl_sets = get_all_available_modes(objects)?;
            let valid = actions
                .iter()
                .map(get_prost_action)
                .all(|action| acl_sets.has_mode(action.into()));
            if !valid {
                return Err(ErrorCode::BindError(
                    "Invalid privilege type for the given object.".to_owned(),
                )
                .into());
            }

            Ok(())
        }
    }
}

fn get_all_available_modes(object: &GrantObjects) -> Result<&AclModeSet> {
    match object {
        GrantObjects::Databases(_) => Ok(&acl::ALL_AVAILABLE_DATABASE_MODES),
        GrantObjects::Schemas(_) => Ok(&acl::ALL_AVAILABLE_SCHEMA_MODES),
        GrantObjects::Sources(_) | GrantObjects::AllSourcesInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_SOURCE_MODES)
        }
        GrantObjects::Mviews(_) | GrantObjects::AllMviewsInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_MVIEW_MODES)
        }
        GrantObjects::Tables(_) | GrantObjects::AllTablesInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_TABLE_MODES)
        }
        GrantObjects::Sinks(_) | GrantObjects::AllSinksInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_SINK_MODES)
        }
        GrantObjects::Views(_) | GrantObjects::AllViewsInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_VIEW_MODES)
        }
        GrantObjects::Functions(_) | GrantObjects::AllFunctionsInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_FUNCTION_MODES)
        }
        GrantObjects::Secrets(_) | GrantObjects::AllSecretsInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_SECRET_MODES)
        }
        GrantObjects::Subscriptions(_) | GrantObjects::AllSubscriptionsInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_SUBSCRIPTION_MODES)
        }
        GrantObjects::Connections(_) | GrantObjects::AllConnectionsInSchema { .. } => {
            Ok(&acl::ALL_AVAILABLE_CONNECTION_MODES)
        }
        _ => Err(
            ErrorCode::BindError("Invalid privilege type for the given object.".to_owned()).into(),
        ),
    }
}

pub fn available_privilege_actions(objects: &GrantObjects) -> Result<Vec<PbAction>> {
    let acl_sets = get_all_available_modes(objects)?;
    Ok(acl_sets.iter().map(Into::into).collect_vec())
}

#[inline(always)]
pub fn get_prost_action(action: &Action) -> PbAction {
    match action {
        Action::Select { .. } => PbAction::Select,
        Action::Insert { .. } => PbAction::Insert,
        Action::Update { .. } => PbAction::Update,
        Action::Delete => PbAction::Delete,
        Action::Connect => PbAction::Connect,
        Action::Create => PbAction::Create,
        Action::Usage => PbAction::Usage,
        Action::Execute => PbAction::Execute,
        _ => unreachable!(),
    }
}

pub fn available_prost_privilege(object: PbObject, for_dml_table: bool) -> PbGrantPrivilege {
    let acl_set = match object {
        PbObject::DatabaseId(_) => &acl::ALL_AVAILABLE_DATABASE_MODES,
        PbObject::SchemaId(_) => &acl::ALL_AVAILABLE_SCHEMA_MODES,
        PbObject::SourceId(_) => &acl::ALL_AVAILABLE_SOURCE_MODES,
        PbObject::TableId(_) => {
            if for_dml_table {
                &acl::ALL_AVAILABLE_TABLE_MODES
            } else {
                &acl::ALL_AVAILABLE_MVIEW_MODES
            }
        }
        PbObject::ViewId(_) => &acl::ALL_AVAILABLE_VIEW_MODES,
        PbObject::SinkId(_) => &acl::ALL_AVAILABLE_SINK_MODES,
        PbObject::SubscriptionId(_) => &acl::ALL_AVAILABLE_SUBSCRIPTION_MODES,
        PbObject::FunctionId(_) => &acl::ALL_AVAILABLE_FUNCTION_MODES,
        PbObject::ConnectionId(_) => &acl::ALL_AVAILABLE_CONNECTION_MODES,
        PbObject::SecretId(_) => &acl::ALL_AVAILABLE_SECRET_MODES,
    };
    let actions = acl_set
        .iter()
        .map(|mode| ActionWithGrantOption {
            action: <AclMode as Into<PbAction>>::into(mode) as i32,
            with_grant_option: false,
            granted_by: DEFAULT_SUPER_USER_ID,
        })
        .collect_vec();
    PbGrantPrivilege {
        action_with_opts: actions,
        object: Some(object),
    }
}
