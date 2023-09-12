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

use anyhow::anyhow;
use risingwave_pb::catalog::{PbDatabase, PbSchema};
use sea_orm::ActiveValue;

use crate::model_v2::{database, schema};
use crate::MetaError;

mod catalog_controller;

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        anyhow!(err).into()
    }
}

impl From<database::Model> for PbDatabase {
    fn from(model: database::Model) -> Self {
        Self {
            id: model.database_id as _,
            name: model.name,
            owner: model.owner_id as _,
        }
    }
}

impl From<PbDatabase> for database::ActiveModel {
    fn from(db: PbDatabase) -> Self {
        Self {
            database_id: ActiveValue::Set(db.id as _),
            name: ActiveValue::Set(db.name),
            owner_id: ActiveValue::Set(db.owner as _),
        }
    }
}

impl From<schema::Model> for PbSchema {
    fn from(model: schema::Model) -> Self {
        Self {
            id: model.schema_id as _,
            name: model.name,
            database_id: model.database_id as _,
            owner: model.owner_id as _,
        }
    }
}

// impl From<user::Model> for PbUserInfo {
//     fn from(model: user::Model) -> Self {
//         Self {
//             id: model.user_id as _,
//             name: model.name,
//             is_super: model.is_super.unwrap_or_default(),
//             can_create_db: model.can_create_db.unwrap_or_default(),
//             can_create_user: model.can_create_user.unwrap_or_default(),
//             can_login: model.can_login.unwrap_or_default(),
//             auth_info: None,
//             grant_privileges: vec![],
//         }
//     }
// }
