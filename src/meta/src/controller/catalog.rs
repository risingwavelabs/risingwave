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
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::catalog::{DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_pb::catalog::PbDatabase;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use sea_orm::sea_query::{
    Alias, CommonTableExpression, Expr, Query, QueryStatementBuilder, SelectStatement, UnionType,
    WithClause,
};
use sea_orm::{
    ActiveModelBehavior, ActiveModelTrait, ActiveValue, ColumnTrait, ConnectOptions,
    ConnectionTrait, Database as SeaDB, DatabaseConnection, DatabaseTransaction, EntityTrait, Iden,
    JoinType, ModelTrait, Order, QueryFilter, Statement, TransactionTrait,
};
use tokio::sync::RwLock;

use crate::controller::{ModelWithObj, ObjectType};
use crate::manager::{DatabaseId, MetaSrvEnv, NotificationVersion, UserId};
use crate::model_v2::prelude::*;
use crate::model_v2::{
    connection, database, function, index, object, object_dependency, schema, sink, source, table,
    view,
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
    pub async fn new(env: MetaSrvEnv, url: &str) -> MetaResult<Self> {
        let mut opts = ConnectOptions::new(url);
        opts.max_connections(20)
            .connect_timeout(Duration::from_secs(10));

        let db = SeaDB::connect(opts).await?;
        Ok(Self {
            env,
            inner: RwLock::new(CatalogControllerInner { db }),
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
    pub async fn snapshot(&self) -> MetaResult<Vec<PbDatabase>> {
        let inner = self.inner.read().await;
        let dbs = Database::find()
            .find_also_related(Object)
            .all(&inner.db)
            .await?;
        let _tables = Table::find()
            .find_also_related(Object)
            .all(&inner.db)
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
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
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

    /// This function will list all the objects that are using the given one. It runs a recursive CTE to find all the dependencies.
    /// The cte and the query is as follows:
    /// ```sql
    /// WITH RECURSIVE used_by_object_ids (used_by) AS
    /// (
    ///     SELECT used_by FROM object_dependency WHERE object_dependency.oid = $1
    ///     UNION ALL
    ///     (
    ///         SELECT object_dependency.used_by
    ///                     FROM object_dependency
    ///             INNER JOIN used_by_object_ids
    ///                     ON used_by_object_ids.used_by = oid
    ///     )
    /// )
    /// SELECT DISTINCT used_by FROM used_by_object_ids ORDER BY used_by DESC;
    /// ```
    async fn list_used_by(&self, obj_id: i32) -> MetaResult<Vec<i32>> {
        let inner = self.inner.read().await;

        let cte_alias = Alias::new("used_by_object_ids");
        let cte_return_alias = Alias::new("used_by");

        let base_query = SelectStatement::new()
            .column(object_dependency::Column::UsedBy)
            .from(object_dependency::Entity)
            .and_where(object_dependency::Column::Oid.eq(obj_id))
            .to_owned();

        let cte_referencing = Query::select()
            .column((object_dependency::Entity, object_dependency::Column::UsedBy))
            .from(object_dependency::Entity)
            .join(
                JoinType::InnerJoin,
                cte_alias.clone(),
                Expr::col((cte_alias.clone(), cte_return_alias.clone()))
                    .equals(object_dependency::Column::Oid),
            )
            .to_owned();

        let common_table_expr = CommonTableExpression::new()
            .query(
                base_query
                    .clone()
                    .union(UnionType::All, cte_referencing)
                    .to_owned(),
            )
            .column(cte_return_alias.clone())
            .table_name(cte_alias.clone())
            .to_owned();

        let query = SelectStatement::new()
            .distinct()
            .column(cte_return_alias.clone())
            .from(cte_alias)
            .order_by(cte_return_alias.clone(), Order::Desc)
            .to_owned()
            .with(
                WithClause::new()
                    .recursive(true)
                    .cte(common_table_expr)
                    .to_owned(),
            )
            .to_owned();

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
            .map(|row| row.try_get("", &cte_return_alias.to_string()).unwrap())
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
        let pb_db = ModelWithObj(db.clone(), db_obj.unwrap()).into();

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
            .from(schema::Entity)
            .and_where(schema::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_table = Query::select()
            .column(table::Column::TableId)
            .from(table::Entity)
            .and_where(table::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_source = Query::select()
            .column(source::Column::SourceId)
            .from(source::Entity)
            .and_where(source::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_sink = Query::select()
            .column(sink::Column::SinkId)
            .from(sink::Entity)
            .and_where(sink::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_index = Query::select()
            .column(index::Column::IndexId)
            .from(index::Entity)
            .and_where(index::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_view = Query::select()
            .column(view::Column::ViewId)
            .from(view::Entity)
            .and_where(view::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_function = Query::select()
            .column(function::Column::FunctionId)
            .from(function::Entity)
            .and_where(function::Column::DatabaseId.eq(database_id as i32))
            .to_owned();
        let query_connection = Query::select()
            .column(connection::Column::ConnectionId)
            .from(connection::Entity)
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
            .one(&mgr.inner.read().await.db)
            .await
            .unwrap()
            .unwrap();
        mgr.drop_database(db.database_id as _).await.unwrap();
    }
}
