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
use risingwave_pb::catalog::{
    PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSink, PbSource, PbTable, PbView,
};
use risingwave_pb::meta::relation::{PbRelationInfo, RelationInfo};
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::meta::{PbRelation, PbRelationGroup};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QuerySelect, TransactionTrait,
};
use tokio::sync::RwLock;

use crate::controller::utils::{
    check_connection_name_duplicate, check_function_signature_duplicate,
    check_relation_name_duplicate, check_schema_name_duplicate, ensure_object_id,
    ensure_object_not_refer, ensure_schema_empty, ensure_user_id, list_used_by, PartialObject,
};
use crate::controller::ObjectModel;
use crate::manager::{MetaSrvEnv, NotificationVersion};
use crate::model_v2::connection::PrivateLinkService;
use crate::model_v2::object::ObjectType;
use crate::model_v2::prelude::*;
use crate::model_v2::{
    connection, database, function, index, object, object_dependency, schema, table, view,
    ConnectionId, DatabaseId, FunctionId, ObjectId, SchemaId, SourceId, TableId, UserId,
};
use crate::rpc::ddl_controller::DropMode;
use crate::{MetaError, MetaResult};

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    env: MetaSrvEnv,
    inner: RwLock<CatalogControllerInner>,
}

#[derive(Clone, Default)]
pub struct ReleaseContext {
    streaming_jobs: Vec<ObjectId>,
    source_ids: Vec<SourceId>,
    connections: Vec<PrivateLinkService>,
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
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
    ) -> MetaResult<object::Model> {
        let active_db = object::ActiveModel {
            oid: Default::default(),
            obj_type: ActiveValue::Set(obj_type),
            owner_id: ActiveValue::Set(owner_id),
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
        ensure_user_id(owner_id, &txn).await?;

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
        ensure_object_id(ObjectType::Database, database_id, &txn).await?;

        let streaming_jobs: Vec<ObjectId> = Object::find()
            .select_only()
            .column(object::Column::Oid)
            .filter(
                object::Column::DatabaseId
                    .eq(Some(database_id))
                    .and(object::Column::ObjType.is_in([ObjectType::Table, ObjectType::Sink])),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let source_ids: Vec<SourceId> = Object::find()
            .select_only()
            .column(object::Column::Oid)
            .filter(
                object::Column::DatabaseId
                    .eq(Some(database_id))
                    .and(object::Column::ObjType.eq(ObjectType::Source)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let connections = Connection::find()
            .inner_join(Object)
            .filter(object::Column::DatabaseId.eq(Some(database_id)))
            .all(&txn)
            .await?
            .into_iter()
            .map(|conn| conn.info)
            .collect_vec();

        // The schema and objects in the database will be delete cascade.
        let res = Object::delete_by_id(database_id).exec(&txn).await?;
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
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, schema.database_id, &txn).await?;
        check_schema_name_duplicate(&schema.name, schema.database_id, &txn).await?;

        let schema_obj = Self::create_object(
            &txn,
            ObjectType::Schema,
            owner_id,
            Some(schema.database_id),
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
        let schema_obj = Object::find_by_id(schema_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;
        if drop_mode == DropMode::Restrict {
            ensure_schema_empty(schema_id, &inner.db).await?;
        }

        let res = Object::delete(object::ActiveModel {
            oid: ActiveValue::Set(schema_id),
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
                    database_id: schema_obj.database_id.unwrap(),
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
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_function.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_function.schema_id, &txn).await?;
        check_function_signature_duplicate(&pb_function, &txn).await?;

        let function_obj = Self::create_object(
            &txn,
            ObjectType::Function,
            owner_id,
            Some(pb_function.database_id),
            Some(pb_function.schema_id),
        )
        .await?;
        pb_function.id = function_obj.oid;
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
        let function_obj = Object::find_by_id(function_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("function", function_id))?;
        ensure_object_not_refer(ObjectType::Function, function_id, &inner.db).await?;

        let res = Object::delete_by_id(function_id).exec(&inner.db).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("function", function_id));
        }

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Function(PbFunction {
                    id: function_id,
                    schema_id: function_obj.schema_id.unwrap(),
                    database_id: function_obj.database_id.unwrap(),
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn create_connection(
        &self,
        mut pb_connection: PbConnection,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_connection.owner;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_connection.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_connection.schema_id, &txn).await?;
        check_connection_name_duplicate(&pb_connection, &txn).await?;

        let conn_obj = Self::create_object(
            &txn,
            ObjectType::Connection,
            owner_id,
            Some(pb_connection.database_id),
            Some(pb_connection.schema_id),
        )
        .await?;
        pb_connection.id = conn_obj.oid;
        let connection: connection::ActiveModel = pb_connection.clone().into();
        connection.insert(&txn).await?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Connection(pb_connection),
            )
            .await;
        Ok(version)
    }

    pub async fn get_connection_by_id(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<PbConnection> {
        let inner = self.inner.read().await;
        let (conn, obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;

        Ok(ObjectModel(conn, obj.unwrap()).into())
    }

    pub async fn drop_connection(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let connection_obj = Object::find_by_id(connection_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;
        ensure_object_not_refer(ObjectType::Connection, connection_id, &inner.db).await?;

        let res = Object::delete_by_id(connection_id).exec(&inner.db).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("connection", connection_id));
        }

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Connection(PbConnection {
                    id: connection_id,
                    schema_id: connection_obj.schema_id.unwrap(),
                    database_id: connection_obj.database_id.unwrap(),
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
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_view.database_id, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_view.schema_id, &txn).await?;
        check_relation_name_duplicate(&pb_view.name, pb_view.database_id, pb_view.schema_id, &txn)
            .await?;

        let view_obj = Self::create_object(
            &txn,
            ObjectType::View,
            owner_id,
            Some(pb_view.database_id),
            Some(pb_view.schema_id),
        )
        .await?;
        pb_view.id = view_obj.oid;
        let view: view::ActiveModel = pb_view.clone().into();
        view.insert(&txn).await?;

        // todo: change `dependent_relations` to `dependent_objects`, which should includes connection and function as well.
        // todo: shall we need to check existence of them Or let database handle it by FOREIGN KEY constraint.
        for obj_id in &pb_view.dependent_relations {
            object_dependency::ActiveModel {
                oid: ActiveValue::Set(*obj_id),
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
        object_type: ObjectType,
        object_id: ObjectId,
        drop_mode: DropMode,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);

        let mut to_drop_objects = match drop_mode {
            DropMode::Cascade => list_used_by(object_id, &txn).await?,
            DropMode::Restrict => {
                ensure_object_not_refer(object_type, object_id, &txn).await?;
                vec![]
            }
        };
        assert!(
            to_drop_objects.iter().all(|obj| matches!(
                obj.obj_type,
                ObjectType::Table | ObjectType::Index | ObjectType::Sink | ObjectType::View
            )),
            "only these objects will depends on others"
        );
        to_drop_objects.push(obj);

        let to_drop_table_ids = to_drop_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table)
            .map(|obj| obj.oid);
        let mut to_drop_streaming_jobs = to_drop_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Sink)
            .map(|obj| obj.oid)
            .collect_vec();
        // todo: record index dependency info in the object dependency table.
        let to_drop_index_ids = to_drop_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Index)
            .map(|obj| obj.oid)
            .collect_vec();

        // Add associated sources.
        let mut to_drop_source_ids: Vec<SourceId> = Table::find()
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(
                table::Column::TableId
                    .is_in(to_drop_table_ids)
                    .and(table::Column::OptionalAssociatedSourceId.is_not_null()),
            )
            .into_tuple()
            .all(&txn)
            .await?;
        let to_drop_source_objs: Vec<PartialObject> = Object::find()
            .filter(object::Column::Oid.is_in(to_drop_source_ids.clone()))
            .into_partial_model()
            .all(&txn)
            .await?;
        to_drop_objects.extend(to_drop_source_objs.clone());
        if object_type == ObjectType::Source {
            to_drop_source_ids.push(object_id);
        }

        // add internal tables.
        let index_table_ids: Vec<TableId> = Index::find()
            .select_only()
            .column(index::Column::IndexTableId)
            .filter(index::Column::IndexId.is_in(to_drop_index_ids))
            .into_tuple()
            .all(&txn)
            .await?;
        to_drop_streaming_jobs.extend(index_table_ids);
        let to_drop_internal_table_objs: Vec<PartialObject> = Object::find()
            .filter(object::Column::Oid.is_in(to_drop_streaming_jobs.clone()))
            .into_partial_model()
            .all(&txn)
            .await?;
        to_drop_objects.extend(to_drop_internal_table_objs);

        // delete all in to_drop_objects.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                object_type.as_str(),
                object_id,
            ));
        }

        // notify about them.
        let relations = to_drop_objects
            .into_iter()
            .map(|obj| match obj.obj_type {
                ObjectType::Table => PbRelation {
                    relation_info: Some(RelationInfo::Table(PbTable {
                        id: obj.oid,
                        schema_id: obj.schema_id.unwrap(),
                        database_id: obj.database_id.unwrap(),
                        ..Default::default()
                    })),
                },
                ObjectType::Source => PbRelation {
                    relation_info: Some(RelationInfo::Source(PbSource {
                        id: obj.oid,
                        schema_id: obj.schema_id.unwrap(),
                        database_id: obj.database_id.unwrap(),
                        ..Default::default()
                    })),
                },
                ObjectType::Sink => PbRelation {
                    relation_info: Some(RelationInfo::Sink(PbSink {
                        id: obj.oid,
                        schema_id: obj.schema_id.unwrap(),
                        database_id: obj.database_id.unwrap(),
                        ..Default::default()
                    })),
                },
                ObjectType::View => PbRelation {
                    relation_info: Some(RelationInfo::View(PbView {
                        id: obj.oid,
                        schema_id: obj.schema_id.unwrap(),
                        database_id: obj.database_id.unwrap(),
                        ..Default::default()
                    })),
                },
                ObjectType::Index => PbRelation {
                    relation_info: Some(RelationInfo::Index(PbIndex {
                        id: obj.oid,
                        schema_id: obj.schema_id.unwrap(),
                        database_id: obj.database_id.unwrap(),
                        ..Default::default()
                    })),
                },
                _ => unreachable!("only relations will be dropped."),
            })
            .collect_vec();
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::RelationGroup(PbRelationGroup { relations }),
            )
            .await;

        Ok((
            ReleaseContext {
                streaming_jobs: to_drop_streaming_jobs,
                source_ids: to_drop_source_ids,
                connections: vec![],
            },
            version,
        ))
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;

    use super::*;

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
        mgr.drop_database(db.database_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_create_view() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "view".to_string(),
            owner: TEST_OWNER_ID,
            sql: "CREATE VIEW view AS SELECT 1".to_string(),
            ..Default::default()
        };
        mgr.create_view(pb_view.clone()).await?;
        assert!(mgr.create_view(pb_view).await.is_err());

        let view = View::find().one(&mgr.inner.read().await.db).await?.unwrap();
        mgr.drop_relation(ObjectType::View, view.view_id, DropMode::Cascade)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_function() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let return_type = risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Int32 as _,
            ..Default::default()
        };
        let pb_function = PbFunction {
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
        };
        mgr.create_function(pb_function.clone()).await?;
        assert!(mgr.create_function(pb_function).await.is_err());

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

        mgr.drop_function(function.function_id).await?;
        assert!(Object::find_by_id(function.function_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }
}
