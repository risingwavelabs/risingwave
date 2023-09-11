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

use std::iter;

use anyhow::anyhow;
use prometheus::core::{Atomic, AtomicI64};
use risingwave_common::catalog::{
    DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_PG,
    DEFAULT_SUPER_USER_FOR_PG_ID, DEFAULT_SUPER_USER_ID, SYSTEM_SCHEMAS,
};
use risingwave_pb::catalog::PbDatabase;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use sea_orm::{
    ActiveModelBehavior, ActiveModelTrait, ActiveValue, ColumnTrait, Database, DatabaseConnection,
    EntityTrait, QueryFilter, TransactionTrait,
};

use crate::manager::{DatabaseId, MetaSrvEnv, NotificationVersion};
use crate::model_v2::prelude;
use crate::{model_v2, MetaError, MetaResult};

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        anyhow!(err).into()
    }
}

// enum Object {
//     Database(database::Model),
//     Schema(schema::Model),
//     Table(schema::Model),
//     User(user::Model),
// }
//
// impl Object {
//     fn obj_type(&self) -> &str {
//         match self {
//             Object::Database(_) => "database",
//             Object::Schema(_) => "schema",
//             Object::Table(_) => "table",
//             Object::User(_) => "user",
//         }
//     }
// }

pub struct MetadataManager {
    env: MetaSrvEnv,
    conn: DatabaseConnection,
}

impl MetadataManager {
    pub async fn new(env: MetaSrvEnv, conn: DatabaseConnection) -> MetaResult<Self> {
        //FIXME: for test only, to remove.
        // let dbs: Vec<model_v2::database::Model> = prelude::Database::find().all(&conn).await.unwrap();
        // dbs.into_iter().for_each(|db| {
        //     println!("db: {:?}", db);
        // });
        let mgr = Self { env, conn };
        mgr.init_user().await?;
        mgr.init_database().await?;
        Ok(mgr)
    }

    // FIXME: should be done in init.sql scripts.
    async fn init_user(&self) -> MetaResult<()> {
        for (id, name) in [
            (DEFAULT_SUPER_USER_ID, DEFAULT_SUPER_USER),
            (DEFAULT_SUPER_USER_FOR_PG_ID, DEFAULT_SUPER_USER_FOR_PG),
        ] {
            if prelude::User::find_by_id(id as i32)
                .one(&self.conn)
                .await?
                .is_some()
            {
                continue;
            }
            let user = model_v2::user::ActiveModel {
                user_id: ActiveValue::Set(id as _),
                name: ActiveValue::Set(name.into()),
                is_super: ActiveValue::Set(Some(true)),
                can_create_db: ActiveValue::Set(Some(true)),
                can_create_user: ActiveValue::Set(Some(true)),
                can_login: ActiveValue::Set(Some(true)),
                ..Default::default()
            };
            user.insert(&self.conn).await?;
        }
        Ok(())
    }

    // FIXME: should be done in init.sql scripts.
    async fn init_database(&self) -> MetaResult<()> {
        if prelude::Database::find()
            .filter(model_v2::database::Column::Name.eq(DEFAULT_DATABASE_NAME))
            .one(&self.conn)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let mut db = model_v2::database::ActiveModel::new();
        db.name = ActiveValue::Set(DEFAULT_DATABASE_NAME.into());
        db.owner_id = ActiveValue::Set(DEFAULT_SUPER_USER_ID as _);
        self.create_database(db).await?;
        Ok(())
    }
}

impl MetadataManager {
    async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }
}

impl MetadataManager {
    // pub async fn snapshot() -> MetaResult<Snapshot, i64> {
    //     //d,s,tdd + self.revision
    //     // [1,2,3] 5
    //     // [1,2,3] 6
    //     // 4 6
    // }
    pub async fn create_database(
        &self,
        db: model_v2::database::ActiveModel,
    ) -> MetaResult<NotificationVersion> {
        let txn = self.conn.begin().await?;
        let db = db.insert(&txn).await?;
        let mut schemas = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            let mut schema = model_v2::schema::ActiveModel::new();
            schema.database_id = ActiveValue::Set(db.database_id);
            schema.name = ActiveValue::Set(schema_name.into());
            schema.owner_id = ActiveValue::Set(db.owner_id);
            schemas.push(schema.insert(&txn).await?);
        }
        txn.commit().await?;

        let mut version = self
            .notify_frontend(Operation::Add, Info::Database(db.into()))
            .await;
        for schema in schemas {
            version = self
                .notify_frontend(Operation::Add, Info::Schema(schema.into()))
                .await;
        }

        Ok(version)
    }

    // todo: return all streaming jobs.
    pub async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<()> {
        let tables = prelude::Table::find()
            .filter(model_v2::table::Column::DatabaseId.eq(database_id as i32))
            .all(&self.conn)
            .await?;
        // 1. unregister source.
        // 2. fragments + actors, streaming manager drop streaming job.
        // 3. connection to drop.

        // let fragments = prelude::Fragment::
        //

        let txn = self.conn.begin().await?;
        let db: model_v2::database::ActiveModel = prelude::Database::find_by_id(database_id as i32)
            .one(&txn)
            .await?
            .ok_or_else(|| anyhow!("database not found"))?
            .into();

        prelude::Table::delete_many()
            .filter(model_v2::table::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::Source::delete_many()
            .filter(model_v2::source::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::Sink::delete_many()
            .filter(model_v2::sink::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::Index::delete_many()
            .filter(model_v2::index::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::Function::delete_many()
            .filter(model_v2::function::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::Connection::delete_many()
            .filter(model_v2::connection::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::View::delete_many()
            .filter(model_v2::view::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::Schema::delete_many()
            .filter(model_v2::schema::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        prelude::Database::delete(db).exec(&txn).await?;

        txn.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::{DbBackend, MockDatabase};
    use super::*;

    #[tokio::test]
    async fn test_create_database() {
        let conn = Database::connect("postgres://postgres:@localhost:5432/postgres")
            .await
            .unwrap();
        // let conn = MockDatabase::new(DbBackend::Postgres).into_connection();
        let mgr = MetadataManager::new(MetaSrvEnv::for_test().await, conn)
            .await
            .unwrap();
        let db = model_v2::database::ActiveModel {
            name: ActiveValue::Set("test".into()),
            owner_id: ActiveValue::Set(1),
            ..Default::default()
        };
        mgr.create_database(db).await.unwrap();
        let db = prelude::Database::find()
            .filter(model_v2::database::Column::Name.eq("test"))
            .one(&mgr.conn)
            .await
            .unwrap()
            .unwrap();
        mgr.drop_database(db.database_id as _).await.unwrap();
    }
}
