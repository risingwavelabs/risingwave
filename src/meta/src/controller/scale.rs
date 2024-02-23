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

use itertools::Itertools;
use risingwave_meta_model_migration::{
    Alias, CommonTableExpression, Expr, MysqlQueryBuilder, PostgresQueryBuilder,
    QueryStatementBuilder, SelectStatement, SqliteQueryBuilder, UnionType, WithClause, WithQuery,
};
use risingwave_meta_model_v2::actor_dispatcher::DispatcherType;
use risingwave_meta_model_v2::prelude::{Actor, ActorDispatcher};
use risingwave_meta_model_v2::{actor, actor_dispatcher, FragmentId};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, JoinType, QueryFilter, QuerySelect,
    QueryTrait, RelationTrait, Statement, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::MetaResult;

/// This function will construct a query using recursive cte to find if dependent objects are already relying on the target table.
///
/// # Examples
///
/// ```
/// use risingwave_meta::controller::scale::construct_no_shuffle_traverse_query;
/// use sea_orm::sea_query::*;
/// use sea_orm::*;
///
/// let query = construct_no_shuffle_traverse_query(vec![2, 3]);
///
/// assert_eq!(query.to_string(MysqlQueryBuilder), r#"WITH RECURSIVE `shuffle_deps` (`fragment_id`) AS (SELECT DISTINCT `actor`.`fragment_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE' AND `actor_dispatcher`.`dispatcher_id` IN (2, 3) UNION ALL (SELECT DISTINCT `actor`.`fragment_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` INNER JOIN `shuffle_deps` ON `shuffle_deps`.`fragment_id` = `actor_dispatcher`.`dispatcher_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE')) SELECT DISTINCT `fragment_id` FROM `shuffle_deps`"#);
/// assert_eq!(query.to_string(PostgresQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id") AS (SELECT DISTINCT "actor"."fragment_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL (SELECT DISTINCT "actor"."fragment_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE')) SELECT DISTINCT "fragment_id" FROM "shuffle_deps""#);
/// assert_eq!(query.to_string(SqliteQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id") AS (SELECT DISTINCT "actor"."fragment_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL SELECT DISTINCT "actor"."fragment_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE') SELECT DISTINCT "fragment_id" FROM "shuffle_deps""#);
/// ```
pub fn construct_no_shuffle_traverse_query(fragment_ids: Vec<FragmentId>) -> WithQuery {
    let cte_alias = Alias::new("shuffle_deps");

    let mut base_query = SelectStatement::new()
        .column((Actor, actor::Column::FragmentId))
        .distinct()
        .from(Actor)
        .inner_join(
            ActorDispatcher,
            Expr::col((Actor, actor::Column::ActorId)).eq(Expr::col((
                ActorDispatcher,
                actor_dispatcher::Column::ActorId,
            ))),
        )
        .and_where(
            Expr::col((ActorDispatcher, actor_dispatcher::Column::DispatcherType))
                .eq(DispatcherType::NoShuffle),
        )
        .and_where(
            Expr::col((ActorDispatcher, actor_dispatcher::Column::DispatcherId))
                .is_in(fragment_ids.clone()),
        )
        .to_owned();

    let cte_referencing = SelectStatement::new()
        .column((Actor, actor::Column::FragmentId))
        .distinct()
        .from(Actor)
        .inner_join(
            ActorDispatcher,
            Expr::col((Actor, actor::Column::ActorId)).eq(Expr::col((
                ActorDispatcher,
                actor_dispatcher::Column::ActorId,
            ))),
        )
        .inner_join(
            cte_alias.clone(),
            Expr::col((cte_alias.clone(), actor::Column::FragmentId)).eq(Expr::col((
                ActorDispatcher,
                actor_dispatcher::Column::DispatcherId,
            ))),
        )
        .and_where(
            Expr::col((ActorDispatcher, actor_dispatcher::Column::DispatcherType))
                .eq(DispatcherType::NoShuffle),
        )
        .to_owned();

    let common_table_expr = CommonTableExpression::new()
        .query(base_query.union(UnionType::All, cte_referencing).to_owned())
        .columns([actor::Column::FragmentId])
        .table_name(cte_alias.clone())
        .to_owned();

    SelectStatement::new()
        .column(actor::Column::FragmentId)
        .distinct()
        .from(cte_alias.clone())
        .to_owned()
        .with(
            WithClause::new()
                .recursive(true)
                .cte(common_table_expr)
                .to_owned(),
        )
        .to_owned()
}

impl CatalogController {
    async fn test<C>(&self, txn: &C, fragment_ids: Vec<FragmentId>) -> MetaResult<()>
    where
        C: ConnectionTrait,
    {
        let query = construct_no_shuffle_traverse_query(fragment_ids.clone());
        let (sql, values) = query.build_any(&*txn.get_database_backend().get_query_builder());
        let res = txn
            .query_all(Statement::from_sql_and_values(
                txn.get_database_backend(),
                sql,
                values,
            ))
            .await?
            .into_iter()
            .map(|res| {
                let fragment_id: FragmentId = res.try_get_by(0).unwrap();
                fragment_id
            })
            .collect_vec();

        println!("frag {:?}", res);

        // println!("cte pg {}", t.to_string(PostgresQueryBuilder));
        // println!("cte my {}", t.to_string(MysqlQueryBuilder));
        // println!("cte lite {}", t.to_string(SqliteQueryBuilder));
        // downstream fragment ids
        // let s1 = Actor::find()
        //     .select_only()
        //     .column(actor::Column::FragmentId)
        //     .distinct()
        //     .join(JoinType::InnerJoin, actor::Relation::ActorDispatcher.def())
        //     .clone()
        //     .filter(actor_dispatcher::Column::DispatcherId.is_in(fragment_ids.clone()));
        //
        // println!("up sql {}", s1.build(DbBackend::Postgres).to_string());
        // let upstream_fragments: Vec<FragmentId> = s1.into_tuple().all(txn).await?;
        //
        // println!("ups {:?}", upstream_fragments);
        //
        // let s2 = Actor::find()
        //     .select_only()
        //     .column(actor_dispatcher::Column::DispatcherId)
        //     .distinct()
        //     .join(JoinType::InnerJoin, actor::Relation::ActorDispatcher.def())
        //     .filter(actor::Column::FragmentId.is_in(fragment_ids));
        //
        // println!("down sql {}", s2.build(DbBackend::Postgres).to_string());
        //
        // let downstream_fragments: Vec<FragmentId> = s2.into_tuple().all(txn).await?;
        //
        // println!("downs {:?}", downstream_fragments);

        // let inner = self.inner.write().await?;
        // let txn = inner.db.begin().await?;
        //
        Ok(())
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use super::*;
    use crate::manager::{MetaOpts, MetaSrvEnv};

    #[tokio::test]
    async fn test_scale() -> MetaResult<()> {
        let srv_env = MetaSrvEnv::for_test().await;
        let mgr = CatalogController::new(srv_env)?;
        let inner = mgr.inner.read().await;
        let txn = inner.db.begin().await?;

        mgr.test(&txn, vec![8, 7, 6]).await.unwrap();

        Ok(())
    }
}

// -- NOSHUFFLE DEPENDENCIES

// -- to downstream
// select distinct a.fragment_id, ap.dispatcher_type, ap.dispatcher_id  from actor a, actor_dispatcher ap where a.actor_id = ap.actor_id and a.fragment_id in (4,8) ;
//
//
// -- to upstream
// select distinct a.fragment_id, ap.dispatcher_type, ap.dispatcher_id  from actor a, actor_dispatcher ap where a.actor_id = ap.actor_id and ap.dispatcher_id in (4,8) ;
