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

use itertools::Itertools;
use risingwave_common::catalog::{DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_pb::catalog::{PbDatabase, PbFunction, PbSchema, PbView};
use risingwave_pb::meta::relation::PbRelationInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QuerySelect, TransactionTrait,
};
use tokio::sync::RwLock;

use crate::controller::utils::{
    check_function_signature_duplicate, check_relation_name_duplicate, check_schema_name_duplicate,
    ensure_object_id, ensure_schema_empty, ensure_user_id, list_used_by,
};
use crate::controller::ObjectModel;
use crate::manager::{
    DatabaseId, FunctionId, MetaSrvEnv, NotificationVersion, RelationIdEnum, SchemaId, UserId,
};
use crate::model_v2::object::ObjectType;
use crate::model_v2::prelude::*;
use crate::model_v2::{database, function, object, object_dependency, schema, view};
use crate::rpc::ddl_controller::DropMode;
use crate::{MetaError, MetaResult};

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    env: MetaSrvEnv,
    inner: RwLock<CatalogControllerInner>,
}

pub struct ReleaseContext {
    streaming_jobs: Vec<i32>,
    source_ids: Vec<i32>,
    connections: Vec<serde_json::Value>,
}

impl CatalogController {
    pub fn new(env: MetaSrvEnv) -> MetaResult<Self> {
        let meta_store = env
            .sql_meta_store()
            .expect("sql meta store is not initialized");
        Ok(Self {
            env,
            inner: RwLock::new(CatalogControllerInner {
                db: meta_store.conn,
            }),
        })
    }
}

struct CatalogControllerInner {
    db: DatabaseConnection,
}

impl CatalogController {
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

    async fn notify_frontend_relation_info(
        &self,
        operation: NotificationOperation,
        relation_info: PbRelationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend_relation_info(operation, relation_info)
            .await
    }
}

impl CatalogController {
    pub fn snapshot(&self) -> MetaResult<()> {
        todo!("snapshot")
    }

    async fn create_object(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
        database_id: Option<i32>,
        schema_id: Option<i32>,
    ) -> MetaResult<object::Model> {
        let active_db = object::ActiveModel {
            oid: Default::default(),
            obj_type: ActiveValue::Set(obj_type),
            owner_id: ActiveValue::Set(owner_id as _),
            schema_id: ActiveValue::Set(schema_id),
            database_id: ActiveValue::Set(database_id),
            initialized_at: Default::default(),
            created_at: Default::default(),
        };
        Ok(active_db.insert(txn).await?)
    }

    pub async fn create_database(&self, db: PbDatabase) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = db.owner;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id as _, &txn).await?;

        let db_obj = Self::create_object(&txn, ObjectType::Database, owner_id, None, None).await?;
        let mut db: database::ActiveModel = db.into();
        db.database_id = ActiveValue::Set(db_obj.oid);
        let db = db.insert(&txn).await?;

        let mut schemas = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            let schema_obj =
                Self::create_object(&txn, ObjectType::Schema, owner_id, Some(db_obj.oid), None)
                    .await?;
            let schema = schema::ActiveModel {
                schema_id: ActiveValue::Set(schema_obj.oid),
                name: ActiveValue::Set(schema_name.into()),
            };
            let schema = schema.insert(&txn).await?;
            schemas.push(ObjectModel(schema, schema_obj).into());
        }
        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Database(ObjectModel(db, db_obj).into()),
            )
            .await;
        for schema in schemas {
            version = self
                .notify_frontend(NotificationOperation::Add, NotificationInfo::Schema(schema))
                .await;
        }

        Ok(version)
    }

    pub async fn drop_database(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Database, database_id as _, &txn).await?;

        let streaming_jobs: Vec<i32> = Object::find()
            .select_only()
            .column(object::Column::Oid)
            .filter(
                object::Column::DatabaseId
                    .eq(Some(database_id as i32))
                    .and(object::Column::ObjType.is_in([ObjectType::Table, ObjectType::Sink])),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let source_ids: Vec<i32> = Object::find()
            .select_only()
            .column(object::Column::Oid)
            .filter(
                object::Column::DatabaseId
                    .eq(Some(database_id as i32))
                    .and(object::Column::ObjType.eq(ObjectType::Source)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let connections = Connection::find()
            .inner_join(Object)
            .filter(object::Column::DatabaseId.eq(Some(database_id as i32)))
            .all(&txn)
            .await?
            .into_iter()
            .flat_map(|conn| conn.info)
            .collect_vec();

        // The schema and objects in the database will be delete cascade.
        let res = Object::delete_by_id(database_id as i32).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("database", database_id));
        }

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Database(PbDatabase {
                    id: database_id,
                    ..Default::default()
                }),
            )
            .await;
        Ok((
            ReleaseContext {
                streaming_jobs,
                source_ids,
                connections,
            },
            version,
        ))
    }

    pub async fn create_schema(&self, schema: PbSchema) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = schema.owner;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id as _, &txn).await?;
        ensure_object_id(ObjectType::Database, schema.database_id as _, &txn).await?;
        check_schema_name_duplicate(&schema.name, schema.database_id as _, &txn).await?;

        let schema_obj = Self::create_object(
            &txn,
            ObjectType::Schema,
            owner_id,
            Some(schema.database_id as _),
            None,
        )
        .await?;
        let mut schema: schema::ActiveModel = schema.into();
        schema.schema_id = ActiveValue::Set(schema_obj.oid);
        let schema = schema.insert(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Schema(ObjectModel(schema, schema_obj).into()),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_schema(
        &self,
        schema_id: SchemaId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let schema_obj = Object::find_by_id(schema_id as i32).one(&inner.db).await?;
        let Some(schema_obj) = schema_obj else {
            return Err(MetaError::catalog_id_not_found("schema", schema_id));
        };
        if matches!(drop_mode, DropMode::Restrict) {
            ensure_schema_empty(schema_id, &inner.db).await?;
        }

        let res = Object::delete(object::ActiveModel {
            oid: ActiveValue::Set(schema_id as _),
            ..Default::default()
        })
        .exec(&inner.db)
        .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("schema", schema_id));
        }

        // todo: update user privileges accordingly.
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Schema(PbSchema {
                    id: schema_id,
                    database_id: schema_obj.database_id.unwrap() as _,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn create_function(
        &self,
        mut pb_function: PbFunction,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_function.owner;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id as _, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_function.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_function.schema_id as _, &txn).await?;
        check_function_signature_duplicate(&pb_function, &txn).await?;

        let function_obj = Self::create_object(
            &txn,
            ObjectType::Function,
            owner_id,
            Some(pb_function.database_id as _),
            Some(pb_function.schema_id as _),
        )
        .await?;
        pb_function.id = function_obj.oid as _;
        let function: function::ActiveModel = pb_function.clone().into();
        function.insert(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Function(pb_function),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_function(&self, function_id: FunctionId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let objects = list_used_by(function_id as _, &inner.db).await?;
        if !objects.is_empty() {
            return Err(MetaError::permission_denied(format!(
                "function is used by {} other objects",
                objects.len()
            )));
        }

        let res = Object::delete_by_id(function_id as i32)
            .exec(&inner.db)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("function", function_id));
        }

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Function(PbFunction {
                    id: function_id,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn create_view(&self, mut pb_view: PbView) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_view.owner;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id as _, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_view.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_view.schema_id as _, &txn).await?;
        check_relation_name_duplicate(
            &pb_view.name,
            pb_view.database_id as _,
            pb_view.schema_id as _,
            &txn,
        )
        .await?;

        let view_obj = Self::create_object(
            &txn,
            ObjectType::View,
            owner_id,
            Some(pb_view.database_id as _),
            Some(pb_view.schema_id as _),
        )
        .await?;
        pb_view.id = view_obj.oid as _;
        let view: view::ActiveModel = pb_view.clone().into();
        view.insert(&txn).await?;

        // todo: change `dependent_relations` to `dependent_objects`, which should includes connection and function as well.
        for obj_id in &pb_view.dependent_relations {
            object_dependency::ActiveModel {
                oid: ActiveValue::Set(*obj_id as _),
                used_by: ActiveValue::Set(view_obj.oid),
                ..Default::default()
            }
            .insert(&txn)
            .await?;
        }

        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Add,
                PbRelationInfo::View(pb_view),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_relation(
        &self,
        _relation: RelationIdEnum,
        _drop_mode: DropMode,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        todo!("implement drop relation.")
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;

    use super::*;
    use crate::manager::SchemaId;

    const TEST_DATABASE_ID: DatabaseId = 1;
    const TEST_SCHEMA_ID: SchemaId = 2;
    const TEST_OWNER_ID: UserId = 1;

    #[tokio::test]
    async fn test_create_database() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let db = PbDatabase {
            name: "test".to_string(),
            owner: DEFAULT_SUPER_USER_ID,
            ..Default::default()
        };
        mgr.create_database(db).await?;

        let db = Database::find()
            .filter(database::Column::Name.eq("test"))
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        mgr.drop_database(db.database_id as _).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_create_function() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let return_type = risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Int32 as _,
            ..Default::default()
        };
        mgr.create_function(PbFunction {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "test_function".to_string(),
            owner: TEST_OWNER_ID,
            arg_types: vec![],
            return_type: Some(return_type.clone()),
            language: "python".to_string(),
            kind: Some(risingwave_pb::catalog::function::Kind::Scalar(
                Default::default(),
            )),
            ..Default::default()
        })
        .await?;

        let function = Function::find()
            .inner_join(Object)
            .filter(
                object::Column::DatabaseId
                    .eq(TEST_DATABASE_ID)
                    .and(object::Column::SchemaId.eq(TEST_SCHEMA_ID))
                    .add(function::Column::Name.eq("test_function")),
            )
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(function.return_type.0, return_type);
        assert_eq!(function.language, "python");

        mgr.drop_function(function.function_id as _).await?;
        assert!(Object::find_by_id(function.function_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }
}
