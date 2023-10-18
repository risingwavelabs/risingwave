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
use sea_orm::{ActiveValue, DatabaseConnection, ModelTrait};

use crate::model_v2::{database, object, schema};
use crate::MetaError;

#[allow(dead_code)]
pub mod catalog;
pub mod cluster;
pub mod system_param;
pub mod utils;

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        if let Some(err) = err.sql_err() {
            return anyhow!(err).into();
        }
        anyhow!(err).into()
    }
}

#[derive(Clone)]
pub struct SqlMetaStore {
    pub(crate) conn: DatabaseConnection,
}

impl SqlMetaStore {
    pub fn new(conn: DatabaseConnection) -> Self {
        Self { conn }
    }

    #[cfg(any(test, feature = "test"))]
    #[cfg(not(madsim))]
    pub async fn for_test() -> Self {
        use model_migration::{Migrator, MigratorTrait};
        let conn = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&conn, None).await.unwrap();
        Self { conn }
    }
}

pub struct ObjectModel<M: ModelTrait>(M, object::Model);

impl From<ObjectModel<database::Model>> for PbDatabase {
    fn from(value: ObjectModel<database::Model>) -> Self {
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

impl From<PbSchema> for schema::ActiveModel {
    fn from(schema: PbSchema) -> Self {
        Self {
            schema_id: ActiveValue::Set(schema.id as _),
            name: ActiveValue::Set(schema.name),
            database_id: ActiveValue::Set(schema.database_id as _),
        }
    }
}

impl From<ObjectModel<schema::Model>> for PbSchema {
    fn from(value: ObjectModel<schema::Model>) -> Self {
        Self {
            id: value.0.schema_id as _,
            name: value.0.name,
            database_id: value.0.database_id as _,
            owner: value.1.owner_id as _,
        }
    }
}
