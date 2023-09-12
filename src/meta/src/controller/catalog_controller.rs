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

use risingwave_common::catalog::{DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_pb::catalog::{PbDatabase, PbSchema};
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use sea_orm::{ActiveModelBehavior, ActiveModelTrait, ActiveValue, ColumnTrait, Database as SeaDB, DatabaseConnection, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter, QueryTrait, TransactionTrait};
use sea_orm::sea_query::Query;
use tokio::sync::RwLock;

use crate::controller::{ModelWithObj, ObjectType};
use crate::manager::{DatabaseId, MetaSrvEnv, NotificationVersion, UserId};
use crate::model_v2::prelude::*;
use crate::model_v2::{
    connection, database, function, index, object, schema, sink, source, table, view,
};
use crate::{MetaError, MetaResult};

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    env: MetaSrvEnv,
    db: DatabaseConnection,
    // todo: replace it with monotonic timestamp.
    revision: RwLock<u64>,
}

impl CatalogController {
    pub async fn new(env: MetaSrvEnv, url: &str) -> MetaResult<Self> {
        let db = SeaDB::connect(url).await?;
        Ok(Self {
            env,
            db,
            revision: RwLock::new(0),
        })
    }
}

impl CatalogController {
    pub async fn get_revision(&self) -> u64 {
        *self.revision.read().await
    }

    async fn notify_frontend(
        &self,
        operation: NotificationOperation,
        info: NotificationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }
}

impl CatalogController {
    pub async fn snapshot(&self) -> MetaResult<Vec<PbDatabase>> {
        let dbs = Database::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        let _tables = Table::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(dbs
            .into_iter()
            .map(|(db, obj)| ModelWithObj(db, obj.unwrap()).into())
            .collect())
    }

    async fn create_object(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
    ) -> MetaResult<object::Model> {
        let mut active_db = object::ActiveModel::new();
        active_db.obj_type = ActiveValue::Set(obj_type.to_string());
        active_db.owner_id = ActiveValue::Set(owner_id as _);
        Ok(active_db.insert(txn).await?)
    }

    pub async fn create_database(&self, db: PbDatabase) -> MetaResult<NotificationVersion> {
        let txn = self.db.begin().await?;
        let owner_id = db.owner;

        let db_obj = Self::create_object(&txn, ObjectType::Database, owner_id).await?;
        let mut db: database::ActiveModel = db.into();
        db.database_id = ActiveValue::Set(db_obj.oid);
        let db = db.insert(&txn).await?;

        let mut schemas = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            let mut schema = schema::ActiveModel::new();
            let schema_obj = Self::create_object(&txn, ObjectType::Schema, owner_id).await?;
            schema.schema_id = ActiveValue::Set(schema_obj.oid);
            schema.database_id = ActiveValue::Set(db.database_id);
            schema.name = ActiveValue::Set(schema_name.into());
            let schema = schema.insert(&txn).await?;

            schemas.push(ModelWithObj(schema, schema_obj).into());
        }
        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Database(ModelWithObj(db, db_obj).into()),
            )
            .await;
        for schema in schemas {
            version = self
                .notify_frontend(NotificationOperation::Add, NotificationInfo::Schema(schema))
                .await;
        }

        Ok(version)
    }

    pub async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<()> {
        let _tables = Table::find()
            .filter(table::Column::DatabaseId.eq(database_id as i32))
            .all(&self.db)
            .await?;
        // 1. unregister source.
        // 2. fragments + actors, streaming manager drop streaming job.
        // 3. connection to drop.

        let txn = self.db.begin().await?;
        let db: database::ActiveModel = Database::find_by_id(database_id as i32)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?
            .into();

        // todo: drop objects.
        // Object::delete_many().filter(object::Column::Oid.in_subquery(
        //
        // )).exec(&txn).await?;
        //

        Table::delete_many()
            .filter(table::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        Source::delete_many()
            .filter(source::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        Sink::delete_many()
            .filter(sink::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        Index::delete_many()
            .filter(index::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        Function::delete_many()
            .filter(function::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        Connection::delete_many()
            .filter(connection::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        View::delete_many()
            .filter(view::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        Schema::delete_many()
            .filter(schema::Column::DatabaseId.eq(database_id as i32))
            .exec(&txn)
            .await?;
        Database::delete(db).exec(&txn).await?;

        txn.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_create_database() {
        // let conn = MockDatabase::new(DbBackend::Postgres).into_connection();
        let mgr = CatalogController::new(
            MetaSrvEnv::for_test().await,
            "postgres://postgres:@localhost:5432/postgres",
        )
        .await
        .unwrap();
        let db = PbDatabase {
            name: "test".to_string(),
            owner: DEFAULT_SUPER_USER_ID,
            ..Default::default()
        };
        mgr.create_database(db).await.unwrap();
        let db = Database::find()
            .filter(database::Column::Name.eq("test"))
            .one(&mgr.db)
            .await
            .unwrap()
            .unwrap();
        mgr.drop_database(db.database_id as _).await.unwrap();
    }
}
