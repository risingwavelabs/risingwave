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
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::*;
use risingwave_meta_model_v2::table::TableType;
use risingwave_meta_model_v2::{
    connection, database, function, index, object, object_dependency, schema, sink, source, table,
    user_privilege, view, ColumnCatalogArray, ConnectionId, DatabaseId, FunctionId, IndexId,
    ObjectId, PrivateLinkService, SchemaId, SourceId, TableId, UserId,
};
use risingwave_pb::catalog::{
    PbComment, PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSink, PbSource, PbTable,
    PbView,
};
use risingwave_pb::meta::relation::PbRelationInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::meta::{PbRelation, PbRelationGroup, PbTableFragments};
use sea_orm::sea_query::SimpleExpr;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, JoinType, QueryFilter, QuerySelect, RelationTrait, TransactionTrait, Value,
};
use tokio::sync::RwLock;

use crate::controller::rename::{alter_relation_rename, alter_relation_rename_refs};
use crate::controller::utils::{
    check_connection_name_duplicate, check_database_name_duplicate,
    check_function_signature_duplicate, check_relation_name_duplicate, check_schema_name_duplicate,
    ensure_object_id, ensure_object_not_refer, ensure_schema_empty, ensure_user_id,
    get_referring_objects, get_referring_objects_cascade, list_user_info_by_ids, PartialObject,
};
use crate::controller::ObjectModel;
use crate::manager::{MetaSrvEnv, NotificationVersion, StreamingJob, IGNORED_NOTIFICATION_VERSION};
use crate::rpc::ddl_controller::DropMode;
use crate::{MetaError, MetaResult};

pub type CatalogControllerRef = Arc<CatalogController>;

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    pub(crate) env: MetaSrvEnv,
    pub(crate) inner: RwLock<CatalogControllerInner>,
}

#[derive(Clone, Default)]
pub struct ReleaseContext {
    pub(crate) streaming_jobs: Vec<ObjectId>,
    pub(crate) source_ids: Vec<SourceId>,
    pub(crate) connections: Vec<PrivateLinkService>,
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

pub(crate) struct CatalogControllerInner {
    pub(crate) db: DatabaseConnection,
}

impl CatalogController {
    pub(crate) async fn notify_frontend(
        &self,
        operation: NotificationOperation,
        info: NotificationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }

    pub(crate) async fn notify_frontend_relation_info(
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
            obj_type: Set(obj_type),
            owner_id: Set(owner_id),
            schema_id: Set(schema_id),
            database_id: Set(database_id),
            initialized_at: Default::default(),
            created_at: Default::default(),
        };
        Ok(active_db.insert(txn).await?)
    }

    pub async fn create_database(&self, db: PbDatabase) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = db.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        check_database_name_duplicate(&db.name, &txn).await?;

        let db_obj = Self::create_object(&txn, ObjectType::Database, owner_id, None, None).await?;
        let mut db: database::ActiveModel = db.into();
        db.database_id = Set(db_obj.oid);
        let db = db.insert(&txn).await?;

        let mut schemas = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            let schema_obj =
                Self::create_object(&txn, ObjectType::Schema, owner_id, Some(db_obj.oid), None)
                    .await?;
            let schema = schema::ActiveModel {
                schema_id: Set(schema_obj.oid),
                name: Set(schema_name.into()),
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

        // Find affect users with privileges on the database and the objects in the database.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .join(JoinType::InnerJoin, user_privilege::Relation::Object.def())
            .filter(
                object::Column::DatabaseId
                    .eq(database_id)
                    .or(user_privilege::Column::Oid.eq(database_id)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        // The schema and objects in the database will be delete cascade.
        let res = Object::delete_by_id(database_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("database", database_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Database(PbDatabase {
                    id: database_id as _,
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
        let owner_id = schema.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
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
        schema.schema_id = Set(schema_obj.oid);
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
        let txn = inner.db.begin().await?;
        let schema_obj = Object::find_by_id(schema_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;
        // TODO: support drop schema cascade.
        if drop_mode == DropMode::Restrict {
            ensure_schema_empty(schema_id, &txn).await?;
        } else {
            return Err(MetaError::permission_denied(
                "drop schema cascade is not supported yet".to_string(),
            ));
        }

        // Find affect users with privileges on the schema and the objects in the schema.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .join(JoinType::InnerJoin, user_privilege::Relation::Object.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .or(user_privilege::Column::Oid.eq(schema_id)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete(object::ActiveModel {
            oid: Set(schema_id),
            ..Default::default()
        })
        .exec(&txn)
        .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("schema", schema_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Schema(PbSchema {
                    id: schema_id as _,
                    database_id: schema_obj.database_id.unwrap() as _,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub fn create_stream_job(
        &self,
        _stream_job: &StreamingJob,
        _table_fragments: &PbTableFragments,
        _internal_tables: Vec<PbTable>,
    ) -> MetaResult<()> {
        todo!()
    }

    pub async fn create_source(&self, mut pb_source: PbSource) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_source.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_source.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_source.schema_id as _, &txn).await?;
        check_relation_name_duplicate(
            &pb_source.name,
            pb_source.database_id as _,
            pb_source.schema_id as _,
            &txn,
        )
        .await?;

        let source_obj = Self::create_object(
            &txn,
            ObjectType::Source,
            owner_id,
            Some(pb_source.database_id as _),
            Some(pb_source.schema_id as _),
        )
        .await?;
        pb_source.id = source_obj.oid as _;
        let source: source::ActiveModel = pb_source.clone().into();
        source.insert(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Add,
                PbRelationInfo::Source(pb_source),
            )
            .await;
        Ok(version)
    }

    pub async fn create_function(
        &self,
        mut pb_function: PbFunction,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_function.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
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
        let txn = inner.db.begin().await?;
        let function_obj = Object::find_by_id(function_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("function", function_id))?;
        ensure_object_not_refer(ObjectType::Function, function_id, &txn).await?;

        // Find affect users with privileges on the function.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(function_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(function_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("function", function_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Function(PbFunction {
                    id: function_id as _,
                    schema_id: function_obj.schema_id.unwrap() as _,
                    database_id: function_obj.database_id.unwrap() as _,
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
        let owner_id = pb_connection.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_connection.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_connection.schema_id as _, &txn).await?;
        check_connection_name_duplicate(&pb_connection, &txn).await?;

        let conn_obj = Self::create_object(
            &txn,
            ObjectType::Connection,
            owner_id,
            Some(pb_connection.database_id as _),
            Some(pb_connection.schema_id as _),
        )
        .await?;
        pb_connection.id = conn_obj.oid as _;
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
        let txn = inner.db.begin().await?;
        let connection_obj = Object::find_by_id(connection_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;
        ensure_object_not_refer(ObjectType::Connection, connection_id, &txn).await?;

        // Find affect users with privileges on the connection.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(connection_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(connection_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("connection", connection_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Connection(PbConnection {
                    id: connection_id as _,
                    schema_id: connection_obj.schema_id.unwrap() as _,
                    database_id: connection_obj.database_id.unwrap() as _,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn create_view(&self, mut pb_view: PbView) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_view.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
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
        // todo: shall we need to check existence of them Or let database handle it by FOREIGN KEY constraint.
        for obj_id in &pb_view.dependent_relations {
            object_dependency::ActiveModel {
                oid: Set(*obj_id as _),
                used_by: Set(view_obj.oid),
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

    pub async fn alter_owner(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        new_owner: UserId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_user_id(new_owner, &txn).await?;

        let obj = Object::find_by_id(object_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        if obj.owner_id == new_owner {
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }
        let mut obj = obj.into_active_model();
        obj.owner_id = Set(new_owner);
        let obj = obj.update(&txn).await?;

        let mut relations = vec![];
        match object_type {
            ObjectType::Database => {
                let db = Database::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("database", object_id))?;

                txn.commit().await?;

                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Database(ObjectModel(db, obj).into()),
                    )
                    .await;
                return Ok(version);
            }
            ObjectType::Schema => {
                let schema = Schema::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("schema", object_id))?;

                txn.commit().await?;

                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Schema(ObjectModel(schema, obj).into()),
                    )
                    .await;
                return Ok(version);
            }
            ObjectType::Table => {
                let table = Table::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;

                // associated source.
                if let Some(associated_source_id) = table.optional_associated_source_id {
                    let src_obj = object::ActiveModel {
                        oid: Set(associated_source_id as _),
                        owner_id: Set(new_owner),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;
                    let source = Source::find_by_id(associated_source_id)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("source", associated_source_id)
                        })?;
                    relations.push(PbRelationInfo::Source(ObjectModel(source, src_obj).into()));
                }

                // indexes.
                let (index_ids, mut table_ids): (Vec<IndexId>, Vec<TableId>) =
                    if table.table_type == TableType::Table {
                        Index::find()
                            .select_only()
                            .columns([index::Column::IndexId, index::Column::IndexTableId])
                            .filter(index::Column::PrimaryTableId.eq(object_id))
                            .into_tuple::<(IndexId, TableId)>()
                            .all(&txn)
                            .await?
                            .into_iter()
                            .unzip()
                    } else {
                        (vec![], vec![])
                    };
                relations.push(PbRelationInfo::Table(ObjectModel(table, obj).into()));

                // internal tables.
                let internal_tables: Vec<TableId> = Table::find()
                    .select_only()
                    .column(table::Column::TableId)
                    .filter(
                        table::Column::BelongsToJobId
                            .is_in(table_ids.iter().cloned().chain(std::iter::once(object_id))),
                    )
                    .into_tuple()
                    .all(&txn)
                    .await?;
                table_ids.extend(internal_tables);

                Object::update_many()
                    .col_expr(
                        object::Column::OwnerId,
                        SimpleExpr::Value(Value::Int(Some(new_owner))),
                    )
                    .filter(
                        object::Column::Oid
                            .is_in(index_ids.iter().cloned().chain(table_ids.iter().cloned())),
                    )
                    .exec(&txn)
                    .await?;

                let index_objs = Index::find()
                    .find_also_related(Object)
                    .filter(index::Column::IndexId.is_in(index_ids))
                    .all(&txn)
                    .await?;
                for (index, index_obj) in index_objs {
                    relations.push(PbRelationInfo::Index(
                        ObjectModel(index, index_obj.unwrap()).into(),
                    ));
                }
                let table_objs = Table::find()
                    .find_also_related(Object)
                    .filter(table::Column::TableId.is_in(table_ids))
                    .all(&txn)
                    .await?;
                for (table, table_obj) in table_objs {
                    relations.push(PbRelationInfo::Table(
                        ObjectModel(table, table_obj.unwrap()).into(),
                    ));
                }
            }
            ObjectType::Source => {
                let source = Source::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
                relations.push(PbRelationInfo::Source(ObjectModel(source, obj).into()));
            }
            ObjectType::Sink => {
                let sink = Sink::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", object_id))?;
                relations.push(PbRelationInfo::Sink(ObjectModel(sink, obj).into()));

                // internal tables.
                let internal_tables: Vec<TableId> = Table::find()
                    .select_only()
                    .column(table::Column::TableId)
                    .filter(table::Column::BelongsToJobId.eq(object_id))
                    .into_tuple()
                    .all(&txn)
                    .await?;

                Object::update_many()
                    .col_expr(
                        object::Column::OwnerId,
                        SimpleExpr::Value(Value::Int(Some(new_owner))),
                    )
                    .filter(object::Column::Oid.is_in(internal_tables.clone()))
                    .exec(&txn)
                    .await?;

                let table_objs = Table::find()
                    .find_also_related(Object)
                    .filter(table::Column::TableId.is_in(internal_tables))
                    .all(&txn)
                    .await?;
                for (table, table_obj) in table_objs {
                    relations.push(PbRelationInfo::Table(
                        ObjectModel(table, table_obj.unwrap()).into(),
                    ));
                }
            }
            ObjectType::View => {
                let view = View::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("view", object_id))?;
                relations.push(PbRelationInfo::View(ObjectModel(view, obj).into()));
            }
            _ => unreachable!("not supported object type: {:?}", object_type),
        };

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::RelationGroup(PbRelationGroup {
                    relations: relations
                        .into_iter()
                        .map(|relation| PbRelation {
                            relation_info: Some(relation),
                        })
                        .collect(),
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn comment_on(&self, comment: PbComment) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Database, comment.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, comment.schema_id as _, &txn).await?;
        let table_obj = Object::find_by_id(comment.table_id as ObjectId)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", comment.table_id))?;

        let table = if let Some(col_idx) = comment.column_index {
            let mut columns: ColumnCatalogArray = Table::find_by_id(comment.table_id as TableId)
                .select_only()
                .column(table::Column::Columns)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", comment.table_id))?;
            let column = columns
                .0
                .get_mut(col_idx as usize)
                .ok_or_else(|| MetaError::catalog_id_not_found("column", col_idx))?;
            let column_desc = column.column_desc.as_mut().ok_or_else(|| {
                anyhow!(
                    "column desc at index {} for table id {} not found",
                    col_idx,
                    comment.table_id
                )
            })?;
            column_desc.description = comment.description;
            table::ActiveModel {
                table_id: Set(comment.table_id as _),
                columns: Set(columns),
                ..Default::default()
            }
            .update(&txn)
            .await?
        } else {
            table::ActiveModel {
                table_id: Set(comment.table_id as _),
                description: Set(comment.description),
                ..Default::default()
            }
            .update(&txn)
            .await?
        };
        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Update,
                PbRelationInfo::Table(ObjectModel(table, table_obj).into()),
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
            DropMode::Cascade => get_referring_objects_cascade(object_id, &txn).await?,
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
        to_drop_objects.extend(to_drop_source_objs);
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

        if !to_drop_streaming_jobs.is_empty() {
            let to_drop_internal_table_objs: Vec<PartialObject> = Object::find()
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .join(JoinType::InnerJoin, object::Relation::Table.def())
                .filter(table::Column::BelongsToJobId.is_in(to_drop_streaming_jobs.clone()))
                .into_partial_model()
                .all(&txn)
                .await?;

            to_drop_objects.extend(to_drop_internal_table_objs);
        }

        // Find affect users with privileges on all this objects.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .into_tuple()
            .all(&txn)
            .await?;

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
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        // notify about them.
        self.notify_users_update(user_infos).await;
        let relations = to_drop_objects
            .into_iter()
            .map(|obj| match obj.obj_type {
                ObjectType::Table => PbRelation {
                    relation_info: Some(PbRelationInfo::Table(PbTable {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                },
                ObjectType::Source => PbRelation {
                    relation_info: Some(PbRelationInfo::Source(PbSource {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                },
                ObjectType::Sink => PbRelation {
                    relation_info: Some(PbRelationInfo::Sink(PbSink {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                },
                ObjectType::View => PbRelation {
                    relation_info: Some(PbRelationInfo::View(PbView {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                },
                ObjectType::Index => PbRelation {
                    relation_info: Some(PbRelationInfo::Index(PbIndex {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
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

    async fn alter_database_name(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        check_database_name_duplicate(name, &txn).await?;

        let active_model = database::ActiveModel {
            database_id: Set(database_id),
            name: Set(name.to_string()),
        };
        let database = active_model.update(&txn).await?;

        let obj = Object::find_by_id(database_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Database(ObjectModel(database, obj).into()),
            )
            .await;
        Ok(version)
    }

    async fn alter_schema_name(
        &self,
        schema_id: SchemaId,
        name: &str,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj = Object::find_by_id(schema_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;
        check_schema_name_duplicate(name, obj.database_id.unwrap(), &txn).await?;

        let active_model = schema::ActiveModel {
            schema_id: Set(schema_id),
            name: Set(name.to_string()),
        };
        let schema = active_model.update(&txn).await?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Schema(ObjectModel(schema, obj).into()),
            )
            .await;
        Ok(version)
    }

    pub async fn alter_name(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        object_name: &str,
    ) -> MetaResult<NotificationVersion> {
        if object_type == ObjectType::Database {
            return self.alter_database_name(object_id as _, object_name).await;
        } else if object_type == ObjectType::Schema {
            return self.alter_schema_name(object_id as _, object_name).await;
        }

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        check_relation_name_duplicate(
            object_name,
            obj.database_id.unwrap(),
            obj.schema_id.unwrap(),
            &txn,
        )
        .await?;

        let mut to_update_relations = vec![];
        // rename relation.
        macro_rules! rename_relation {
            ($entity:ident, $table:ident, $identity:ident, $object_id:expr) => {{
                let (mut relation, obj) = $entity::find_by_id($object_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .unwrap();
                let old_name = relation.name.clone();
                relation.name = object_name.into();
                relation.definition = alter_relation_rename(&relation.definition, object_name);
                let active_model = $table::ActiveModel {
                    $identity: Set(relation.$identity),
                    name: Set(object_name.into()),
                    definition: Set(relation.definition.clone()),
                    ..Default::default()
                };
                active_model.update(&txn).await?;
                to_update_relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::$entity(
                        ObjectModel(relation, obj.unwrap()).into(),
                    )),
                });
                old_name
            }};
        }

        let old_name = match object_type {
            ObjectType::Table => rename_relation!(Table, table, table_id, object_id),
            ObjectType::Source => rename_relation!(Source, source, source_id, object_id),
            ObjectType::Sink => rename_relation!(Sink, sink, sink_id, object_id),
            ObjectType::View => rename_relation!(View, view, view_id, object_id),
            ObjectType::Index => {
                let (mut index, obj) = Index::find_by_id(object_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .unwrap();
                index.name = object_name.into();
                let index_table_id = index.index_table_id;

                // the name of index and its associated table is the same.
                let active_model = index::ActiveModel {
                    index_id: Set(index.index_id),
                    name: Set(object_name.into()),
                    ..Default::default()
                };
                active_model.update(&txn).await?;
                to_update_relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Index(
                        ObjectModel(index, obj.unwrap()).into(),
                    )),
                });
                rename_relation!(Table, table, table_id, index_table_id)
            }
            _ => unreachable!("only relation name can be altered."),
        };

        // rename referring relation name.
        macro_rules! rename_relation_ref {
            ($entity:ident, $table:ident, $identity:ident, $object_id:expr) => {{
                let (mut relation, obj) = $entity::find_by_id($object_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .unwrap();
                relation.definition =
                    alter_relation_rename_refs(&relation.definition, &old_name, object_name);
                let active_model = $table::ActiveModel {
                    $identity: Set(relation.$identity),
                    definition: Set(relation.definition.clone()),
                    ..Default::default()
                };
                active_model.update(&txn).await?;
                to_update_relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::$entity(
                        ObjectModel(relation, obj.unwrap()).into(),
                    )),
                });
            }};
        }
        let objs = get_referring_objects(object_id, &txn).await?;
        for obj in objs {
            match obj.obj_type {
                ObjectType::Table => rename_relation_ref!(Table, table, table_id, obj.oid),
                ObjectType::Sink => rename_relation_ref!(Sink, sink, sink_id, obj.oid),
                ObjectType::View => rename_relation_ref!(View, view, view_id, obj.oid),
                ObjectType::Index => {
                    let index_table_id: Option<TableId> = Index::find_by_id(obj.oid)
                        .select_only()
                        .column(index::Column::IndexTableId)
                        .into_tuple()
                        .one(&txn)
                        .await?;
                    rename_relation_ref!(Table, table, table_id, index_table_id.unwrap());
                }
                _ => bail!("only table, sink, view and index depend on other objects."),
            }
        }
        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::RelationGroup(PbRelationGroup {
                    relations: to_update_relations,
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn alter_source_column(
        &self,
        pb_source: PbSource,
    ) -> MetaResult<NotificationVersion> {
        let source_id = pb_source.id as SourceId;
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let original_version: i64 = Source::find_by_id(source_id)
            .select_only()
            .column(source::Column::Version)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
        if original_version + 1 != pb_source.version as i64 {
            return Err(MetaError::permission_denied(
                "source version is stale".to_string(),
            ));
        }

        let source: source::ActiveModel = pb_source.clone().into();
        source.update(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Update,
                PbRelationInfo::Source(pb_source),
            )
            .await;
        Ok(version)
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use risingwave_meta_model_v2::ViewId;

    use super::*;

    const TEST_DATABASE_ID: DatabaseId = 1;
    const TEST_SCHEMA_ID: SchemaId = 2;
    const TEST_OWNER_ID: UserId = 1;

    #[tokio::test]
    async fn test_database_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let pb_database = PbDatabase {
            name: "db1".to_string(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_database(pb_database).await?;

        let database_id: DatabaseId = Database::find()
            .select_only()
            .column(database::Column::DatabaseId)
            .filter(database::Column::Name.eq("db1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Database, database_id, "db2")
            .await?;
        let database = Database::find_by_id(database_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(database.name, "db2");

        mgr.drop_database(database_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let pb_schema = PbSchema {
            database_id: TEST_DATABASE_ID as _,
            name: "schema1".to_string(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_schema(pb_schema.clone()).await?;
        assert!(mgr.create_schema(pb_schema).await.is_err());

        let schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("schema1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Schema, schema_id, "schema2")
            .await?;
        let schema = Schema::find_by_id(schema_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(schema.name, "schema2");
        mgr.drop_schema(schema_id, DropMode::Restrict).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_view() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "view".to_string(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view AS SELECT 1".to_string(),
            ..Default::default()
        };
        mgr.create_view(pb_view.clone()).await?;
        assert!(mgr.create_view(pb_view).await.is_err());

        let view = View::find().one(&mgr.inner.read().await.db).await?.unwrap();
        mgr.drop_relation(ObjectType::View, view.view_id, DropMode::Cascade)
            .await?;
        assert!(View::find_by_id(view.view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

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
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "test_function".to_string(),
            owner: TEST_OWNER_ID as _,
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

    #[tokio::test]
    async fn test_alter_relation_rename() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await)?;
        let pb_source = PbSource {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "s1".to_string(),
            owner: TEST_OWNER_ID as _,
            definition: r#"CREATE SOURCE s1 (v1 int) with (
  connector = 'kafka',
  topic = 'kafka_alter',
  properties.bootstrap.server = 'message_queue:29092',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON"#
                .to_string(),
            ..Default::default()
        };
        mgr.create_source(pb_source).await?;
        let source_id: SourceId = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .filter(source::Column::Name.eq("s1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "view_1".to_string(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view_1 AS SELECT v1 FROM s1".to_string(),
            dependent_relations: vec![source_id as _],
            ..Default::default()
        };
        mgr.create_view(pb_view).await?;
        let view_id: ViewId = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .filter(view::Column::Name.eq("view_1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Source, source_id, "s2").await?;
        let source = Source::find_by_id(source_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(source.name, "s2");
        assert_eq!(
            source.definition,
            "CREATE SOURCE s2 (v1 INT) WITH (\
  connector = 'kafka', \
  topic = 'kafka_alter', \
  properties.bootstrap.server = 'message_queue:29092', \
  scan.startup.mode = 'earliest'\
) FORMAT PLAIN ENCODE JSON"
        );

        let view = View::find_by_id(view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(
            view.definition,
            "CREATE VIEW view_1 AS SELECT v1 FROM s2 AS s1"
        );

        mgr.drop_relation(ObjectType::Source, source_id, DropMode::Cascade)
            .await?;
        assert!(View::find_by_id(view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }
}
