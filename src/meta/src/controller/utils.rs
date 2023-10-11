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
use model_migration::WithQuery;
use risingwave_pb::catalog::PbFunction;
use sea_orm::sea_query::{
    Alias, CommonTableExpression, Expr, Query, QueryStatementBuilder, SelectStatement, UnionType,
    WithClause,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, Order, PaginatorTrait, QueryFilter,
    Statement,
};

use crate::manager::SchemaId;
use crate::model_v2::object::ObjectType;
use crate::model_v2::prelude::*;
use crate::model_v2::{function, object, object_dependency, schema, table, DataTypeArray};
use crate::{MetaError, MetaResult};

/// This function will construct a query using recursive cte to find all objects that are used by the given object.
///
/// # Examples
///
/// ```
/// use risingwave_meta::controller::utils::construct_obj_dependency_query;
/// use sea_orm::sea_query::*;
/// use sea_orm::*;
///
/// let query = construct_obj_dependency_query(1, "used_by");
///
/// assert_eq!(
///     query.to_string(MysqlQueryBuilder),
///     r#"WITH RECURSIVE `used_by_object_ids` (`used_by`) AS (SELECT `used_by` FROM `object_dependency` WHERE `object_dependency`.`oid` = 1 UNION ALL (SELECT `object_dependency`.`used_by` FROM `object_dependency` INNER JOIN `used_by_object_ids` ON `used_by_object_ids`.`used_by` = `oid`)) SELECT DISTINCT `used_by` FROM `used_by_object_ids` ORDER BY `used_by` DESC"#
/// );
/// assert_eq!(
///     query.to_string(PostgresQueryBuilder),
///     r#"WITH RECURSIVE "used_by_object_ids" ("used_by") AS (SELECT "used_by" FROM "object_dependency" WHERE "object_dependency"."oid" = 1 UNION ALL (SELECT "object_dependency"."used_by" FROM "object_dependency" INNER JOIN "used_by_object_ids" ON "used_by_object_ids"."used_by" = "oid")) SELECT DISTINCT "used_by" FROM "used_by_object_ids" ORDER BY "used_by" DESC"#
/// );
/// assert_eq!(
///     query.to_string(SqliteQueryBuilder),
///     r#"WITH RECURSIVE "used_by_object_ids" ("used_by") AS (SELECT "used_by" FROM "object_dependency" WHERE "object_dependency"."oid" = 1 UNION ALL SELECT "object_dependency"."used_by" FROM "object_dependency" INNER JOIN "used_by_object_ids" ON "used_by_object_ids"."used_by" = "oid") SELECT DISTINCT "used_by" FROM "used_by_object_ids" ORDER BY "used_by" DESC"#
/// );
/// ```
pub fn construct_obj_dependency_query(obj_id: i32, column: &str) -> WithQuery {
    let cte_alias = Alias::new("used_by_object_ids");
    let cte_return_alias = Alias::new(column);

    let base_query = SelectStatement::new()
        .column(object_dependency::Column::UsedBy)
        .from(ObjectDependency)
        .and_where(object_dependency::Column::Oid.eq(obj_id))
        .to_owned();

    let cte_referencing = Query::select()
        .column((ObjectDependency, object_dependency::Column::UsedBy))
        .from(ObjectDependency)
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

    SelectStatement::new()
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
        .to_owned()
}

/// List all objects that are using the given one. It runs a recursive CTE to find all the dependencies.
pub async fn list_used_by<C>(obj_id: i32, db: &C) -> MetaResult<Vec<i32>>
where
    C: ConnectionTrait,
{
    let query = construct_obj_dependency_query(obj_id, "used_by");
    let (sql, values) = query.build_any(&*db.get_database_backend().get_query_builder());
    let res = db
        .query_all(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            values,
        ))
        .await?;

    let ids: Vec<i32> = res
        .into_iter()
        .map(|row| row.try_get("", "user_by").unwrap())
        .collect();
    Ok(ids)
}

pub async fn ensure_object_id<C>(object_type: ObjectType, obj_id: i32, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Object::find_by_id(obj_id).count(db).await?;
    if count == 0 {
        return Err(MetaError::catalog_id_not_found(
            object_type.as_str(),
            obj_id as u32,
        ));
    }
    Ok(())
}

pub async fn ensure_user_id<C>(user_id: i32, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = User::find_by_id(user_id).count(db).await?;
    if count == 0 {
        return Err(anyhow!("user {} was concurrently dropped", user_id).into());
    }
    Ok(())
}

pub async fn check_function_signature_duplicate<C>(
    pb_function: &PbFunction,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Function::find()
        .inner_join(Object)
        .filter(
            object::Column::DatabaseId
                .eq(pb_function.database_id as i32)
                .and(object::Column::SchemaId.eq(pb_function.schema_id as i32))
                .and(function::Column::Name.eq(&pb_function.name))
                .and(function::Column::ArgTypes.eq(DataTypeArray(pb_function.arg_types.clone()))),
        )
        .count(db)
        .await?;
    if count > 0 {
        assert_eq!(count, 1);
        return Err(MetaError::catalog_duplicated("function", &pb_function.name));
    }
    Ok(())
}

pub async fn check_relation_name_duplicate<C>(
    name: &str,
    database_id: i32,
    schema_id: i32,
    db: &C,
) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    macro_rules! check_duplicated {
        ($obj_type:expr, $entity:expr, $table:expr) => {
            let count = Object::find()
                .inner_join($entity)
                .filter(
                    object::Column::DatabaseId
                        .eq(Some(database_id))
                        .and(object::Column::SchemaId.eq(Some(schema_id)))
                        .and(table::Column::Name.eq(name)),
                )
                .count(db)
                .await?;
            if count != 0 {
                return Err(MetaError::catalog_duplicated($obj_type.as_str(), name));
            }
        };
    }
    check_duplicated!(ObjectType::Table, Table, table);
    check_duplicated!(ObjectType::Source, Source, source);
    check_duplicated!(ObjectType::Sink, Sink, sink);
    check_duplicated!(ObjectType::Index, Index, index);
    check_duplicated!(ObjectType::View, View, view);

    Ok(())
}

pub async fn check_schema_name_duplicate<C>(name: &str, database_id: i32, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Object::find()
        .inner_join(Schema)
        .filter(
            object::Column::ObjType
                .eq(ObjectType::Schema)
                .and(object::Column::DatabaseId.eq(Some(database_id)))
                .and(schema::Column::Name.eq(name)),
        )
        .count(db)
        .await?;
    if count != 0 {
        return Err(MetaError::catalog_duplicated("schema", name));
    }

    Ok(())
}

pub async fn ensure_schema_empty<C>(schema_id: SchemaId, db: &C) -> MetaResult<()>
where
    C: ConnectionTrait,
{
    let count = Object::find()
        .filter(object::Column::SchemaId.eq(Some(schema_id as i32)))
        .count(db)
        .await?;
    if count != 0 {
        return Err(MetaError::permission_denied("schema is not empty".into()));
    }

    Ok(())
}
