// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{TableOption, DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_common::{bail, current_cluster_version};
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::*;
use risingwave_meta_model_v2::table::TableType;
use risingwave_meta_model_v2::{
    connection, database, function, index, object, object_dependency, schema, sink, source,
    streaming_job, table, user_privilege, view, ActorId, ColumnCatalogArray, ConnectionId,
    CreateType, DatabaseId, FragmentId, FunctionId, IndexId, JobStatus, ObjectId,
    PrivateLinkService, SchemaId, SourceId, StreamSourceInfo, StreamingParallelism, TableId,
    UserId,
};
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::catalog::{
    PbComment, PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSink, PbSource, PbTable,
    PbView,
};
use risingwave_pb::meta::cancel_creating_jobs_request::PbCreatingJobInfo;
use risingwave_pb::meta::relation::PbRelationInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Info, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::{PbRelation, PbRelationGroup};
use risingwave_pb::user::PbUserInfo;
use sea_orm::sea_query::{Expr, SimpleExpr};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, JoinType, PaginatorTrait, QueryFilter, QuerySelect, RelationTrait,
    TransactionTrait, Value,
};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::controller::rename::{alter_relation_rename, alter_relation_rename_refs};
use crate::controller::utils::{
    check_connection_name_duplicate, check_database_name_duplicate,
    check_function_signature_duplicate, check_relation_name_duplicate, check_schema_name_duplicate,
    ensure_object_id, ensure_object_not_refer, ensure_schema_empty, ensure_user_id,
    get_fragment_mappings, get_fragment_mappings_by_jobs, get_referring_objects,
    get_referring_objects_cascade, get_user_privilege, list_user_info_by_ids,
    resolve_source_register_info_for_jobs, PartialObject,
};
use crate::controller::ObjectModel;
use crate::manager::{Catalog, MetaSrvEnv, NotificationVersion, IGNORED_NOTIFICATION_VERSION};
use crate::rpc::ddl_controller::DropMode;
use crate::stream::SourceManagerRef;
use crate::{MetaError, MetaResult};

pub type CatalogControllerRef = Arc<CatalogController>;

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    pub(crate) env: MetaSrvEnv,
    pub(crate) inner: RwLock<CatalogControllerInner>,
}

#[derive(Clone, Default)]
pub struct ReleaseContext {
    /// Dropped state table list, need to unregister from hummock.
    pub(crate) state_table_ids: Vec<TableId>,
    /// Dropped source list, need to unregister from source manager.
    pub(crate) source_ids: Vec<SourceId>,
    /// Dropped connection list, need to delete from vpc endpoints.
    pub(crate) connections: Vec<PrivateLinkService>,

    /// Dropped fragments that are fetching data from the target source.
    pub(crate) source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// Dropped actors.
    pub(crate) removed_actors: HashSet<ActorId>,
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

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_inner_read_guard(&self) -> RwLockReadGuard<'_, CatalogControllerInner> {
        self.inner.read().await
    }
}

pub struct CatalogControllerInner {
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
    pub(crate) async fn create_object(
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
            initialized_at_cluster_version: Set(Some(current_cluster_version())),
            created_at_cluster_version: Set(Some(current_cluster_version())),
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

        let streaming_jobs: Vec<ObjectId> = StreamingJob::find()
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(object::Column::DatabaseId.eq(Some(database_id)))
            .into_tuple()
            .all(&txn)
            .await?;

        let (source_fragments, removed_actors) =
            resolve_source_register_info_for_jobs(&txn, streaming_jobs.clone()).await?;

        let state_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(
                table::Column::BelongsToJobId
                    .is_in(streaming_jobs.clone())
                    .or(table::Column::TableId.is_in(streaming_jobs.clone())),
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
        let fragment_mappings = get_fragment_mappings_by_jobs(&txn, streaming_jobs).await?;

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
        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;
        Ok((
            ReleaseContext {
                state_table_ids,
                source_ids,
                connections,
                source_fragments,
                removed_actors,
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

    pub async fn list_background_creating_mviews(&self) -> MetaResult<Vec<table::Model>> {
        let inner = self.inner.read().await;
        let tables = Table::find()
            .join(JoinType::LeftJoin, table::Relation::Object1.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                table::Column::TableType
                    .eq(TableType::MaterializedView)
                    .and(
                        streaming_job::Column::CreateType
                            .eq(CreateType::Background)
                            .and(streaming_job::Column::JobStatus.eq(JobStatus::Creating)),
                    ),
            )
            .all(&inner.db)
            .await?;
        Ok(tables)
    }

    pub async fn has_any_streaming_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = streaming_job::Entity::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    /// `clean_dirty_creating_jobs` cleans up creating jobs that are creating in Foreground mode or in Initial status.
    pub async fn clean_dirty_creating_jobs(&self) -> MetaResult<ReleaseContext> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let creating_job_ids: Vec<ObjectId> = streaming_job::Entity::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(
                streaming_job::Column::JobStatus.eq(JobStatus::Initial).or(
                    streaming_job::Column::JobStatus
                        .eq(JobStatus::Creating)
                        .and(streaming_job::Column::CreateType.eq(CreateType::Foreground)),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;
        if creating_job_ids.is_empty() {
            return Ok(ReleaseContext::default());
        }

        let associated_source_ids: Vec<SourceId> = Table::find()
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(
                table::Column::TableId
                    .is_in(creating_job_ids.clone())
                    .and(table::Column::OptionalAssociatedSourceId.is_not_null()),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let state_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(
                table::Column::BelongsToJobId
                    .is_in(creating_job_ids.clone())
                    .or(table::Column::TableId.is_in(creating_job_ids.clone())),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let to_delete_objs: HashSet<ObjectId> = creating_job_ids
            .clone()
            .into_iter()
            .chain(state_table_ids.clone().into_iter())
            .collect();

        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_delete_objs))
            .exec(&txn)
            .await?;
        assert!(res.rows_affected > 0);
        txn.commit().await?;

        Ok(ReleaseContext {
            state_table_ids,
            source_ids: associated_source_ids,
            ..Default::default()
        })
    }

    /// `finish_streaming_job` marks job related objects as `Created` and notify frontend.
    pub async fn finish_streaming_job(&self, job_id: ObjectId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let job_type = Object::find_by_id(job_id)
            .select_only()
            .column(object::Column::ObjType)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;

        // update `created_at` as now() and `created_at_cluster_version` as current cluster version.
        let res = Object::update_many()
            .col_expr(object::Column::CreatedAt, Expr::current_timestamp().into())
            .col_expr(
                object::Column::CreatedAtClusterVersion,
                current_cluster_version().into(),
            )
            .filter(object::Column::Oid.eq(job_id))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("streaming job", job_id));
        }

        // mark the target stream job as `Created`.
        let job = streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Created),
            ..Default::default()
        };
        job.update(&txn).await?;

        // notify frontend: job, internal tables.
        let internal_table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::BelongsToJobId.eq(job_id))
            .all(&txn)
            .await?;
        let mut relations = internal_table_objs
            .into_iter()
            .map(|(table, obj)| PbRelation {
                relation_info: Some(PbRelationInfo::Table(
                    ObjectModel(table, obj.unwrap()).into(),
                )),
            })
            .collect_vec();

        match job_type {
            ObjectType::Table => {
                let (table, obj) = Table::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", job_id))?;
                if let Some(source_id) = table.optional_associated_source_id {
                    let (src, obj) = Source::find_by_id(source_id)
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
                    relations.push(PbRelation {
                        relation_info: Some(PbRelationInfo::Source(
                            ObjectModel(src, obj.unwrap()).into(),
                        )),
                    });
                }
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Table(
                        ObjectModel(table, obj.unwrap()).into(),
                    )),
                });
            }
            ObjectType::Sink => {
                let (sink, obj) = Sink::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", job_id))?;
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Sink(
                        ObjectModel(sink, obj.unwrap()).into(),
                    )),
                });
            }
            ObjectType::Index => {
                let (index, obj) = Index::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("index", job_id))?;
                {
                    let (table, obj) = Table::find_by_id(index.index_table_id)
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("table", index.index_table_id)
                        })?;
                    relations.push(PbRelation {
                        relation_info: Some(PbRelationInfo::Table(
                            ObjectModel(table, obj.unwrap()).into(),
                        )),
                    });
                }
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Index(
                        ObjectModel(index, obj.unwrap()).into(),
                    )),
                });
            }
            ObjectType::Source => {
                let (source, obj) = Source::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", job_id))?;
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Source(
                        ObjectModel(source, obj.unwrap()).into(),
                    )),
                });
            }
            _ => unreachable!("invalid job type: {:?}", job_type),
        }

        let fragment_mapping = get_fragment_mappings(&txn, job_id).await?;
        txn.commit().await?;

        self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
            .await;
        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::RelationGroup(PbRelationGroup { relations }),
            )
            .await;

        Ok(version)
    }

    pub async fn create_source(
        &self,
        mut pb_source: PbSource,
        source_manager_ref: Option<SourceManagerRef>,
    ) -> MetaResult<NotificationVersion> {
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
        Source::insert(source).exec(&txn).await?;

        if let Some(src_manager) = source_manager_ref {
            let ret = src_manager.register_source(&pb_source).await;
            if let Err(e) = ret {
                txn.rollback().await?;
                return Err(e.into());
            }
        }
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
        Function::insert(function).exec(&txn).await?;
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
        Connection::insert(connection).exec(&txn).await?;

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
    ) -> MetaResult<(NotificationVersion, PbConnection)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let (conn, conn_obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
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

        let pb_connection: PbConnection = ObjectModel(conn, conn_obj.unwrap()).into();

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Connection(pb_connection.clone()),
            )
            .await;
        Ok((version, pb_connection))
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
        View::insert(view).exec(&txn).await?;

        // todo: change `dependent_relations` to `dependent_objects`, which should includes connection and function as well.
        // todo: shall we need to check existence of them Or let database handle it by FOREIGN KEY constraint.
        for obj_id in &pb_view.dependent_relations {
            ObjectDependency::insert(object_dependency::ActiveModel {
                oid: Set(*obj_id as _),
                used_by: Set(view_obj.oid),
                ..Default::default()
            })
            .exec(&txn)
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

                if !index_ids.is_empty() || !table_ids.is_empty() {
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
                }

                if !table_ids.is_empty() {
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
                // FIXME: frontend will update index/primary table from cache, requires apply updates of indexes after tables.
                if !index_ids.is_empty() {
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

    pub async fn alter_schema(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        new_schema: SchemaId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Schema, new_schema, &txn).await?;

        let obj = Object::find_by_id(object_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        if obj.schema_id == Some(new_schema) {
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }
        let database_id = obj.database_id.unwrap();

        let mut relations = vec![];
        match object_type {
            ObjectType::Table => {
                let table = Table::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;
                check_relation_name_duplicate(&table.name, database_id, new_schema, &txn).await?;
                let (associated_src_id, table_type) =
                    (table.optional_associated_source_id, table.table_type);

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::Table(ObjectModel(table, obj).into()));

                // associated source.
                if let Some(associated_source_id) = associated_src_id {
                    let src_obj = object::ActiveModel {
                        oid: Set(associated_source_id as _),
                        schema_id: Set(Some(new_schema)),
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
                let (index_ids, (index_names, mut table_ids)): (
                    Vec<IndexId>,
                    (Vec<String>, Vec<TableId>),
                ) = if table_type == TableType::Table {
                    Index::find()
                        .select_only()
                        .columns([
                            index::Column::IndexId,
                            index::Column::Name,
                            index::Column::IndexTableId,
                        ])
                        .filter(index::Column::PrimaryTableId.eq(object_id))
                        .into_tuple::<(IndexId, String, TableId)>()
                        .all(&txn)
                        .await?
                        .into_iter()
                        .map(|(id, name, t_id)| (id, (name, t_id)))
                        .unzip()
                } else {
                    (vec![], (vec![], vec![]))
                };

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

                if !index_ids.is_empty() || !table_ids.is_empty() {
                    for index_name in index_names {
                        check_relation_name_duplicate(&index_name, database_id, new_schema, &txn)
                            .await?;
                    }

                    Object::update_many()
                        .col_expr(
                            object::Column::SchemaId,
                            SimpleExpr::Value(Value::Int(Some(new_schema))),
                        )
                        .filter(
                            object::Column::Oid
                                .is_in(index_ids.iter().cloned().chain(table_ids.iter().cloned())),
                        )
                        .exec(&txn)
                        .await?;
                }

                if !table_ids.is_empty() {
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
                if !index_ids.is_empty() {
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
                }
            }
            ObjectType::Source => {
                let source = Source::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
                check_relation_name_duplicate(&source.name, database_id, new_schema, &txn).await?;

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::Source(ObjectModel(source, obj).into()));
            }
            ObjectType::Sink => {
                let sink = Sink::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", object_id))?;
                check_relation_name_duplicate(&sink.name, database_id, new_schema, &txn).await?;

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::Sink(ObjectModel(sink, obj).into()));

                // internal tables.
                let internal_tables: Vec<TableId> = Table::find()
                    .select_only()
                    .column(table::Column::TableId)
                    .filter(table::Column::BelongsToJobId.eq(object_id))
                    .into_tuple()
                    .all(&txn)
                    .await?;

                if !internal_tables.is_empty() {
                    Object::update_many()
                        .col_expr(
                            object::Column::SchemaId,
                            SimpleExpr::Value(Value::Int(Some(new_schema))),
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
            }
            ObjectType::View => {
                let view = View::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("view", object_id))?;
                check_relation_name_duplicate(&view.name, database_id, new_schema, &txn).await?;

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::View(ObjectModel(view, obj).into()));
            }
            ObjectType::Function => {
                let function = Function::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("function", object_id))?;

                let mut pb_function: PbFunction = ObjectModel(function, obj).into();
                pb_function.schema_id = new_schema as _;
                check_function_signature_duplicate(&pb_function, &txn).await?;

                object::ActiveModel {
                    oid: Set(object_id),
                    schema_id: Set(Some(new_schema)),
                    ..Default::default()
                }
                .update(&txn)
                .await?;

                txn.commit().await?;
                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Function(pb_function),
                    )
                    .await;
                return Ok(version);
            }
            ObjectType::Connection => {
                let connection = Connection::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("connection", object_id))?;

                let mut pb_connection: PbConnection = ObjectModel(connection, obj).into();
                pb_connection.schema_id = new_schema as _;
                check_connection_name_duplicate(&pb_connection, &txn).await?;

                object::ActiveModel {
                    oid: Set(object_id),
                    schema_id: Set(Some(new_schema)),
                    ..Default::default()
                }
                .update(&txn)
                .await?;

                txn.commit().await?;
                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Connection(pb_connection),
                    )
                    .await;
                return Ok(version);
            }
            _ => unreachable!("not supported object type: {:?}", object_type),
        }

        txn.commit().await?;
        let version = self
            .notify_frontend(
                Operation::Update,
                Info::RelationGroup(PbRelationGroup {
                    relations: relations
                        .into_iter()
                        .map(|relation_info| PbRelation {
                            relation_info: Some(relation_info),
                        })
                        .collect_vec(),
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
                if obj.obj_type == ObjectType::Table {
                    let indexes = get_referring_objects(object_id, &txn).await?;
                    assert!(
                        indexes.iter().all(|obj| obj.obj_type == ObjectType::Index),
                        "only index could be dropped in restrict mode"
                    );
                    indexes
                } else {
                    vec![]
                }
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
            .filter(|obj| obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Index)
            .map(|obj| obj.oid);
        let mut to_drop_streaming_jobs = to_drop_objects
            .iter()
            .filter(|obj| {
                obj.obj_type == ObjectType::Table
                    || obj.obj_type == ObjectType::Sink
                    || obj.obj_type == ObjectType::Index
            })
            .map(|obj| obj.oid)
            .collect_vec();

        // cdc source streaming job.
        if object_type == ObjectType::Source {
            let source_info: Option<StreamSourceInfo> = Source::find_by_id(object_id)
                .select_only()
                .column(source::Column::SourceInfo)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
            if let Some(source_info) = source_info
                && source_info.into_inner().cdc_source_job
            {
                to_drop_streaming_jobs.push(object_id);
            }
        }

        let creating = StreamingJob::find()
            .filter(
                streaming_job::Column::JobStatus
                    .ne(JobStatus::Created)
                    .and(streaming_job::Column::JobId.is_in(to_drop_streaming_jobs.clone())),
            )
            .count(&txn)
            .await?;
        if creating != 0 {
            return Err(MetaError::permission_denied(format!(
                "can not drop {creating} creating streaming job, please cancel them firstly"
            )));
        }

        let mut to_drop_state_table_ids = to_drop_table_ids.clone().collect_vec();

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

            to_drop_state_table_ids.extend(to_drop_internal_table_objs.iter().map(|obj| obj.oid));
            to_drop_objects.extend(to_drop_internal_table_objs);
        }

        let (source_fragments, removed_actors) =
            resolve_source_register_info_for_jobs(&txn, to_drop_streaming_jobs.clone()).await?;
        let fragment_mappings = get_fragment_mappings_by_jobs(&txn, to_drop_streaming_jobs).await?;

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
        let mut relations = vec![];
        for obj in to_drop_objects {
            match obj.obj_type {
                ObjectType::Table => relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Table(PbTable {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                }),
                ObjectType::Source => relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Source(PbSource {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                }),
                ObjectType::Sink => relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Sink(PbSink {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                }),
                ObjectType::View => relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::View(PbView {
                        id: obj.oid as _,
                        schema_id: obj.schema_id.unwrap() as _,
                        database_id: obj.database_id.unwrap() as _,
                        ..Default::default()
                    })),
                }),
                ObjectType::Index => {
                    relations.push(PbRelation {
                        relation_info: Some(PbRelationInfo::Index(PbIndex {
                            id: obj.oid as _,
                            schema_id: obj.schema_id.unwrap() as _,
                            database_id: obj.database_id.unwrap() as _,
                            ..Default::default()
                        })),
                    });
                    relations.push(PbRelation {
                        relation_info: Some(PbRelationInfo::Table(PbTable {
                            id: obj.oid as _,
                            schema_id: obj.schema_id.unwrap() as _,
                            database_id: obj.database_id.unwrap() as _,
                            ..Default::default()
                        })),
                    });
                }
                _ => unreachable!("only relations will be dropped."),
            }
        }
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::RelationGroup(PbRelationGroup { relations }),
            )
            .await;
        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;

        Ok((
            ReleaseContext {
                state_table_ids: to_drop_state_table_ids,
                source_ids: to_drop_source_ids,
                connections: vec![],
                source_fragments,
                removed_actors,
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
                let obj = obj.unwrap();
                let old_name = relation.name.clone();
                relation.name = object_name.into();
                if obj.obj_type != ObjectType::View {
                    relation.definition = alter_relation_rename(&relation.definition, object_name);
                }
                let active_model = $table::ActiveModel {
                    $identity: Set(relation.$identity),
                    name: Set(object_name.into()),
                    definition: Set(relation.definition.clone()),
                    ..Default::default()
                };
                active_model.update(&txn).await?;
                to_update_relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::$entity(ObjectModel(relation, obj).into())),
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
                let old_name = rename_relation!(Table, table, table_id, index_table_id);

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
                old_name
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
        // TODO: For sink into table. when sink into table is ready.
        // if object_type == ObjectType::Table {
        //     let incoming_sinks: Vec<_> = Table::find_by_id(object_id)
        //         .select_only()
        //         .column(table::Column::IncomingSinks)
        //         .into_tuple()
        //         .one(&txn)
        //         .await?
        //         .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;
        //     objs.extend(incoming_sinks.into_iter().map(|id| PartialObject {
        //         oid: id as _,
        //         obj_type: ObjectType::Sink,
        //         ..Default::default()
        //     }));
        // }

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

    pub async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let inner = self.inner.read().await;
        inner.list_databases().await
    }

    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        inner.list_all_state_tables().await
    }

    pub async fn list_all_state_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        inner.list_all_state_table_ids().await
    }

    pub async fn list_readonly_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.ne(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_dml_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.eq(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_tables_by_type(&self, table_type: TableType) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableType.eq(table_type))
            .all(&inner.db)
            .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let inner = self.inner.read().await;
        inner.list_sources().await
    }

    pub async fn list_source_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<SourceId>> {
        let inner = self.inner.read().await;
        let source_ids: Vec<SourceId> = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .join(JoinType::InnerJoin, source::Relation::Object.def())
            .filter(object::Column::SchemaId.eq(schema_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(source_ids)
    }

    pub async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        inner.list_sinks().await
    }

    pub async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let inner = self.inner.read().await;
        inner.list_views().await
    }

    pub async fn get_table_by_name(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> MetaResult<Option<PbTable>> {
        let inner = self.inner.read().await;
        let table_obj = Table::find()
            .find_also_related(Object)
            .join(JoinType::InnerJoin, object::Relation::Database.def())
            .filter(
                table::Column::Name
                    .eq(table_name)
                    .and(database::Column::Name.eq(database_name)),
            )
            .one(&inner.db)
            .await?;
        Ok(table_obj.map(|(table, obj)| ObjectModel(table, obj.unwrap()).into()))
    }

    pub async fn get_table_by_ids(&self, table_ids: Vec<TableId>) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(table_ids))
            .all(&inner.db)
            .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    pub async fn find_creating_streaming_job_ids(
        &self,
        infos: Vec<PbCreatingJobInfo>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;

        type JobKey = (DatabaseId, SchemaId, String);

        // Index table is already included if we still assign the same name for index table as the index.
        let creating_tables: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_sinks: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, sink::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut job_mapping: HashMap<JobKey, ObjectId> = creating_tables
            .into_iter()
            .chain(creating_sinks.into_iter())
            .map(|(id, name, database_id, schema_id)| ((database_id, schema_id, name), id))
            .collect();

        Ok(infos
            .into_iter()
            .flat_map(|info| {
                job_mapping.remove(&(
                    info.database_id as _,
                    info.schema_id as _,
                    info.name.clone(),
                ))
            })
            .collect())
    }

    pub async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let inner = self.inner.read().await;
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&inner.db)
            .await?;
        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<TableId, TableOption>> {
        let inner = self.inner.read().await;
        let table_options: Vec<(TableId, Option<u32>)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::RetentionSeconds])
            .into_tuple::<(TableId, Option<u32>)>()
            .all(&inner.db)
            .await?;

        Ok(table_options
            .into_iter()
            .map(|(id, retention_seconds)| (id, TableOption { retention_seconds }))
            .collect())
    }

    pub async fn get_all_created_streaming_parallelisms(
        &self,
    ) -> MetaResult<HashMap<ObjectId, StreamingParallelism>> {
        let inner = self.inner.read().await;

        let job_parallelisms = StreamingJob::find()
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .select_only()
            .columns([
                streaming_job::Column::JobId,
                streaming_job::Column::Parallelism,
            ])
            .into_tuple::<(ObjectId, StreamingParallelism)>()
            .all(&inner.db)
            .await?;

        Ok(job_parallelisms
            .into_iter()
            .collect::<HashMap<ObjectId, StreamingParallelism>>())
    }

    pub async fn get_table_name_type_mapping(
        &self,
    ) -> MetaResult<HashMap<TableId, (String, String)>> {
        let inner = self.inner.read().await;
        let table_name_types: Vec<(TableId, String, TableType)> = Table::find()
            .select_only()
            .columns([
                table::Column::TableId,
                table::Column::Name,
                table::Column::TableType,
            ])
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_name_types
            .into_iter()
            .map(|(id, name, table_type)| {
                (
                    id,
                    (
                        name,
                        PbTableType::from(table_type).as_str_name().to_string(),
                    ),
                )
            })
            .collect())
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;

        // created table ids.
        let mut table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::LeftJoin, table::Relation::Object1.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&inner.db)
            .await?;

        // internal table ids.
        let internal_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::BelongsToJobId.is_in(table_ids.clone()))
            .into_tuple()
            .all(&inner.db)
            .await?;
        table_ids.extend(internal_table_ids);

        Ok(table_ids)
    }
}

impl CatalogControllerInner {
    pub async fn snapshot(&self) -> MetaResult<(Catalog, Vec<PbUserInfo>)> {
        let databases = self.list_databases().await?;
        let schemas = self.list_schemas().await?;
        let tables = self.list_tables().await?;
        let sources = self.list_sources().await?;
        let sinks = self.list_sinks().await?;
        let indexes = self.list_indexes().await?;
        let views = self.list_views().await?;
        let functions = self.list_functions().await?;
        let connections = self.list_connections().await?;

        let users = self.list_users().await?;

        Ok((
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                indexes,
                views,
                functions,
                connections,
            ),
            users,
        ))
    }

    async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let db_objs = Database::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        Ok(db_objs
            .into_iter()
            .map(|(db, obj)| ObjectModel(db, obj.unwrap()).into())
            .collect())
    }

    async fn list_schemas(&self) -> MetaResult<Vec<PbSchema>> {
        let schema_objs = Schema::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(schema_objs
            .into_iter()
            .map(|(schema, obj)| ObjectModel(schema, obj.unwrap()).into())
            .collect())
    }

    async fn list_users(&self) -> MetaResult<Vec<PbUserInfo>> {
        let mut user_infos: Vec<PbUserInfo> = User::find()
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        for user_info in &mut user_infos {
            user_info.grant_privileges = get_user_privilege(user_info.id as _, &self.db).await?;
        }
        Ok(user_infos)
    }

    /// `list_all_tables` return all tables and internal tables.
    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let table_objs = Table::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    /// `list_all_tables` return all ids of state tables.
    pub async fn list_all_state_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(table_ids)
    }

    /// `list_tables` return all `CREATED` tables and internal tables that belong to `CREATED` streaming jobs.
    async fn list_tables(&self) -> MetaResult<Vec<PbTable>> {
        let table_objs = Table::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .all(&self.db)
            .await?;

        let created_streaming_job_ids: Vec<ObjectId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&self.db)
            .await?;

        let internal_table_objs = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::TableType
                    .eq(TableType::Internal)
                    .and(table::Column::BelongsToJobId.is_in(created_streaming_job_ids)),
            )
            .all(&self.db)
            .await?;

        Ok(table_objs
            .into_iter()
            .chain(internal_table_objs.into_iter())
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    /// `list_sources` return all sources and `CREATED` ones if contains any streaming jobs.
    async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let source_objs = Source::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                streaming_job::Column::JobStatus
                    .is_null()
                    .or(streaming_job::Column::JobStatus.eq(JobStatus::Created)),
            )
            .all(&self.db)
            .await?;
        // TODO: filter out inner connector source that are still under creating.

        Ok(source_objs
            .into_iter()
            .map(|(source, obj)| ObjectModel(source, obj.unwrap()).into())
            .collect())
    }

    /// `list_sinks` return all `CREATED` sinks.
    async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .all(&self.db)
            .await?;

        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| ObjectModel(sink, obj.unwrap()).into())
            .collect())
    }

    async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let view_objs = View::find().find_also_related(Object).all(&self.db).await?;

        Ok(view_objs
            .into_iter()
            .map(|(view, obj)| ObjectModel(view, obj.unwrap()).into())
            .collect())
    }

    /// `list_indexes` return all `CREATED` indexes.
    async fn list_indexes(&self) -> MetaResult<Vec<PbIndex>> {
        let index_objs = Index::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .all(&self.db)
            .await?;

        Ok(index_objs
            .into_iter()
            .map(|(index, obj)| ObjectModel(index, obj.unwrap()).into())
            .collect())
    }

    async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap()).into())
            .collect())
    }

    async fn list_functions(&self) -> MetaResult<Vec<PbFunction>> {
        let func_objs = Function::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(func_objs
            .into_iter()
            .map(|(func, obj)| ObjectModel(func, obj.unwrap()).into())
            .collect())
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
        mgr.create_source(pb_source, None).await?;
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
