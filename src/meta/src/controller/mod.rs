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
use parse_display::Display;
use risingwave_pb::catalog::{PbDatabase, PbSchema, PbTable};
use sea_orm::{ActiveValue, EntityTrait, ModelTrait};

use crate::model_v2::{database, object, schema};
use crate::MetaError;

mod catalog_controller;

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        anyhow!(err).into()
    }
}

#[derive(Clone, Display, Debug)]
#[display(style = "UPPERCASE")]
enum ObjectType {
    Database,
    Schema,
    Table,
    Source,
    Sink,
    View,
    Index,
    Function,
    Connection,
}

pub struct ModelWithObj<M: ModelTrait>(M, object::Model);

impl From<ModelWithObj<database::Model>> for PbDatabase {
    fn from(value: ModelWithObj<database::Model>) -> Self {
        Self {
            id: value.0.database_id as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
        }
    }
}

impl From<PbDatabase> for database::ActiveModel {
    fn from(db: PbDatabase) -> Self {
        Self {
            database_id: ActiveValue::Set(db.id as _),
            name: ActiveValue::Set(db.name),
        }
    }
}

impl From<ModelWithObj<schema::Model>> for PbSchema {
    fn from(value: ModelWithObj<schema::Model>) -> Self {
        Self {
            id: value.0.schema_id as _,
            name: value.0.name,
            database_id: value.0.database_id as _,
            owner: value.1.owner_id as _,
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
