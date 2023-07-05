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

use itertools::Itertools;
use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, PbAction, PbObject};
use risingwave_pb::user::PbGrantPrivilege;
use risingwave_sqlparser::ast::{Action, GrantObjects, Privileges};

// TODO: add user_privilege mod under user manager and move check and expand logic there, and bitmap
// impl for privilege check.
static AVAILABLE_ACTION_ON_DATABASE: &[Action] = &[Action::Connect, Action::Create];
static AVAILABLE_ACTION_ON_SCHEMA: &[Action] = &[Action::Create];
static AVAILABLE_ACTION_ON_SOURCE: &[Action] = &[
    Action::Select { columns: None },
    Action::Update { columns: None },
    Action::Insert { columns: None },
    Action::Delete,
];
static AVAILABLE_ACTION_ON_MVIEW: &[Action] = &[Action::Select { columns: None }];
static AVAILABLE_ACTION_ON_VIEW: &[Action] = AVAILABLE_ACTION_ON_MVIEW;
static AVAILABLE_ACTION_ON_SINK: &[Action] = &[];
static AVAILABLE_ACTION_ON_FUNCTION: &[Action] = &[];

pub fn check_privilege_type(privilege: &Privileges, objects: &GrantObjects) -> Result<()> {
    match privilege {
        Privileges::All { .. } => Ok(()),
        Privileges::Actions(actions) => {
            let valid = match objects {
                GrantObjects::Databases(_) => actions
                    .iter()
                    .all(|action| AVAILABLE_ACTION_ON_DATABASE.contains(action)),
                GrantObjects::Schemas(_) => actions
                    .iter()
                    .all(|action| AVAILABLE_ACTION_ON_SCHEMA.contains(action)),
                GrantObjects::Sources(_) | GrantObjects::AllSourcesInSchema { .. } => actions
                    .iter()
                    .all(|action| AVAILABLE_ACTION_ON_SOURCE.contains(action)),
                GrantObjects::Mviews(_) | GrantObjects::AllMviewsInSchema { .. } => actions
                    .iter()
                    .all(|action| AVAILABLE_ACTION_ON_MVIEW.contains(action)),
                GrantObjects::Sinks(_) => actions
                    .iter()
                    .all(|action| AVAILABLE_ACTION_ON_SINK.contains(action)),
                GrantObjects::Sequences(_)
                | GrantObjects::AllSequencesInSchema { .. }
                | GrantObjects::Tables(_)
                | GrantObjects::AllTablesInSchema { .. } => true,
            };
            if !valid {
                return Err(ErrorCode::BindError(
                    "Invalid privilege type for the given object.".to_string(),
                )
                .into());
            }

            Ok(())
        }
    }
}

pub fn available_privilege_actions(objects: &GrantObjects) -> Result<Vec<Action>> {
    match objects {
        GrantObjects::Databases(_) => Ok(AVAILABLE_ACTION_ON_DATABASE.to_vec()),
        GrantObjects::Schemas(_) => Ok(AVAILABLE_ACTION_ON_SCHEMA.to_vec()),
        GrantObjects::Sources(_) | GrantObjects::AllSourcesInSchema { .. } => {
            Ok(AVAILABLE_ACTION_ON_SOURCE.to_vec())
        }
        GrantObjects::Mviews(_) | GrantObjects::AllMviewsInSchema { .. } => {
            Ok(AVAILABLE_ACTION_ON_MVIEW.to_vec())
        }
        _ => Err(
            ErrorCode::BindError("Invalid privilege type for the given object.".to_string()).into(),
        ),
    }
}

#[inline(always)]
pub fn get_prost_action(action: &Action) -> PbAction {
    match action {
        Action::Select { .. } => PbAction::Select,
        Action::Insert { .. } => PbAction::Insert,
        Action::Update { .. } => PbAction::Update,
        Action::Delete { .. } => PbAction::Delete,
        Action::Connect => PbAction::Connect,
        Action::Create => PbAction::Create,
        _ => unreachable!(),
    }
}

pub fn available_prost_privilege(object: PbObject) -> PbGrantPrivilege {
    let actions = match object {
        PbObject::DatabaseId(_) => AVAILABLE_ACTION_ON_DATABASE.to_vec(),
        PbObject::SchemaId(_) => AVAILABLE_ACTION_ON_SCHEMA.to_vec(),
        PbObject::SourceId(_) | PbObject::AllSourcesSchemaId { .. } => {
            AVAILABLE_ACTION_ON_SOURCE.to_vec()
        }
        PbObject::TableId(_) | PbObject::AllTablesSchemaId { .. } => {
            AVAILABLE_ACTION_ON_MVIEW.to_vec()
        }
        PbObject::ViewId(_) => AVAILABLE_ACTION_ON_VIEW.to_vec(),
        PbObject::SinkId(_) => AVAILABLE_ACTION_ON_SINK.to_vec(),
        PbObject::FunctionId(_) => AVAILABLE_ACTION_ON_FUNCTION.to_vec(),
    };
    let actions = actions
        .iter()
        .map(|action| ActionWithGrantOption {
            action: get_prost_action(action) as i32,
            with_grant_option: false,
            granted_by: DEFAULT_SUPER_USER_ID,
        })
        .collect_vec();
    PbGrantPrivilege {
        action_with_opts: actions,
        object: Some(object),
    }
}
