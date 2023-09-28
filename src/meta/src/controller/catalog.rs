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
use risingwave_pb::catalog::{PbDatabase, PbSchema};
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use sea_orm::sea_query::{Expr, Query, QueryStatementBuilder, UnionType};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter, Statement, TransactionTrait,
};
use tokio::sync::RwLock;

use crate::controller::utils::construct_obj_dependency_query;
use crate::controller::ObjectModel;
use crate::manager::{DatabaseId, MetaSrvEnv, NotificationVersion, UserId};
use crate::model_v2::object::ObjectType;
use crate::model_v2::prelude::*;
use crate::model_v2::{
    connection, database, function, index, object, schema, sink, source, table, view,
};
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
}

impl CatalogController {
    pub fn snapshot(&self) -> MetaResult<()> {
        todo!("snapshot")
    }

    async fn create_object(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
    ) -> MetaResult<object::Model> {
        let active_db = object::ActiveModel {
            oid: Default::default(),
            obj_type: ActiveValue::Set(obj_type),
            owner_id: ActiveValue::Set(owner_id as _),
            initialized_at: Default::default(),
            created_at: Default::default(),
        };
        Ok(active_db.insert(txn).await?)
    }

    pub async fn create_database(&self, db: PbDatabase) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let owner_id = db.owner;

        let db_obj = Self::create_object(&txn, ObjectType::Database, owner_id).await?;
        let mut db: database::ActiveModel = db.into();
        db.database_id = ActiveValue::Set(db_obj.oid);
        let db = db.insert(&txn).await?;

        let mut schemas = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            let schema_obj = Self::create_object(&txn, ObjectType::Schema, owner_id).await?;
            let schema = schema::ActiveModel {
                schema_id: ActiveValue::Set(schema_obj.oid),
                name: ActiveValue::Set(schema_name.into()),
                database_id: ActiveValue::Set(db.database_id),
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

    /// List all objects that are using the given one. It runs a recursive CTE to find all the dependencies.
    async fn list_used_by(&self, obj_id: i32) -> MetaResult<Vec<i32>> {
        let inner = self.inner.read().await;

        let query = construct_obj_dependency_query(obj_id, "used_by");
        let (sql, values) = query.build_any(&*inner.db.get_database_backend().get_query_builder());
        let res = inner
            .db
            .query_all(Statement::from_sql_and_values(
                inner.db.get_database_backend(),
                sql,
                values,
            ))
            .await?;

        let ids: Vec<i32> = res
            .into_iter()
            .map(|row| row.try_get("", "user_by").unwrap())
            .collect_vec();
        Ok(ids)
    }

    pub async fn drop_database(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let db = Database::find_by_id(database_id as i32)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?;
        let db_obj = db.find_related(Object).one(&txn).await?;
        let pb_db = ObjectModel(db.clone(), db_obj.unwrap()).into();

        let tables = db.find_related(Table).all(&txn).await?;
        let sinks = db.find_related(Sink).all(&txn).await?;
        let indexes = db.find_related(Index).all(&txn).await?;
        let streaming_jobs = tables
            .into_iter()
            .map(|t| t.table_id)
            .chain(sinks.into_iter().map(|s| s.sink_id))
            .chain(indexes.into_iter().map(|i| i.index_id))
            .collect::<Vec<_>>();
        let source_ids = db
            .find_related(Source)
            .all(&txn)
            .await?
            .into_iter()
            .map(|s| s.source_id)
            .collect_vec();
        let connections = db
            .find_related(Connection)
            .all(&txn)
            .await?
            .into_iter()
            .flat_map(|c| c.info)
            .collect_vec();

        let mut query_schema = Query::select()
            .column(schema::Column::SchemaId)
            .from(Schema)
            .and_where(schema::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_table = Query::select()
            .column(table::Column::TableId)
            .from(Table)
            .and_where(table::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_source = Query::select()
            .column(source::Column::SourceId)
            .from(Source)
            .and_where(source::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_sink = Query::select()
            .column(sink::Column::SinkId)
            .from(Sink)
            .and_where(sink::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_index = Query::select()
            .column(index::Column::IndexId)
            .from(Index)
            .and_where(index::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_view = Query::select()
            .column(view::Column::ViewId)
            .from(View)
            .and_where(view::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_function = Query::select()
            .column(function::Column::FunctionId)
            .from(Function)
            .and_where(function::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_connection = Query::select()
            .column(connection::Column::ConnectionId)
            .from(Connection)
            .and_where(connection::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_all = query_schema
            .union(UnionType::All, query_table)
            .union(UnionType::All, query_source)
            .union(UnionType::All, query_sink)
            .union(UnionType::All, query_index)
            .union(UnionType::All, query_view)
            .union(UnionType::All, query_function)
            .union(UnionType::All, query_connection)
            .union(
                UnionType::All,
                Query::select()
                    .expr(Expr::value(database_id as i32))
                    .to_owned(),
            )
            .to_owned();
        // drop related objects, the relations will be dropped cascade.
        Object::delete_many()
            .filter(object::Column::Oid.in_subquery(query_all))
            .exec(&txn)
            .await?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Database(pb_db),
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
        let txn = inner.db.begin().await?;
        let owner_id = schema.owner;

        // todo: whether to check existence of database and user, or let the database do it?
        let schema_obj = Self::create_object(&txn, ObjectType::Schema, owner_id).await?;
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
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;

    use super::*;

    #[tokio::test]
    #[cfg(not(madsim))]
    async fn test_create_database() {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).unwrap();
        let db = PbDatabase {
            name: "test".to_string(),
            owner: DEFAULT_SUPER_USER_ID,
            ..Default::default()
        };
        mgr.create_database(db).await.unwrap();
        let db = Database::find()
            .filter(database::Column::Name.eq("test"))
            .one(&mgr.inner.read().await.db)
            .await
            .unwrap()
            .unwrap();
        mgr.drop_database(db.database_id as _).await.unwrap();
    }
}
