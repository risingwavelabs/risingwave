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
use risingwave_pb::catalog::{PbDatabase, PbSchema};
use sea_orm::{ActiveValue, ModelTrait};

use crate::model_v2::{database, object, schema};
use crate::MetaError;

#[allow(dead_code)]
mod catalog;
#[allow(dead_code)]
mod system_param;

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        anyhow!(err).into()
    }
}

#[derive(Clone, Display, Debug)]
#[display(style = "UPPERCASE")]
#[allow(dead_code)]
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
