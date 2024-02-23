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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_meta_model_migration::{
    Alias, CommonTableExpression, Expr, MysqlQueryBuilder, PostgresQueryBuilder,
    QueryStatementBuilder, SelectStatement, SqliteQueryBuilder, UnionType, WithClause, WithQuery,
};
use risingwave_meta_model_v2::actor_dispatcher::DispatcherType;
use risingwave_meta_model_v2::prelude::{Actor, ActorDispatcher, Fragment};
use risingwave_meta_model_v2::{actor, actor_dispatcher, fragment, ActorId, FragmentId};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, JoinType, QueryFilter, QuerySelect,
    QueryTrait, RelationTrait, Statement, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::MetaResult;

/// This function will construct a query using recursive cte to find no_shuffle relation graph for target fragments.
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
/// assert_eq!(query.to_string(MysqlQueryBuilder), r#"WITH RECURSIVE `shuffle_deps` (`fragment_id`, `dispatcher_type`, `dispatcher_id`) AS (SELECT DISTINCT `actor`.`fragment_id`, `actor_dispatcher`.`dispatcher_type`, `actor_dispatcher`.`dispatcher_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE' AND `actor_dispatcher`.`dispatcher_id` IN (2, 3) UNION ALL (SELECT DISTINCT `actor`.`fragment_id`, `actor_dispatcher`.`dispatcher_type`, `actor_dispatcher`.`dispatcher_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` INNER JOIN `shuffle_deps` ON `shuffle_deps`.`fragment_id` = `actor_dispatcher`.`dispatcher_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE')) SELECT DISTINCT `fragment_id`, `dispatcher_type`, `dispatcher_id` FROM `shuffle_deps`"#);
/// assert_eq!(query.to_string(PostgresQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id", "dispatcher_type", "dispatcher_id") AS (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE')) SELECT DISTINCT "fragment_id", "dispatcher_type", "dispatcher_id" FROM "shuffle_deps""#);
/// assert_eq!(query.to_string(SqliteQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id", "dispatcher_type", "dispatcher_id") AS (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE') SELECT DISTINCT "fragment_id", "dispatcher_type", "dispatcher_id" FROM "shuffle_deps""#);
/// ```
pub fn construct_no_shuffle_traverse_query(fragment_ids: Vec<FragmentId>) -> WithQuery {
    let cte_alias = Alias::new("shuffle_deps");

    let mut base_query = SelectStatement::new()
        .column((Actor, actor::Column::FragmentId))
        .column((ActorDispatcher, actor_dispatcher::Column::DispatcherType))
        .column((ActorDispatcher, actor_dispatcher::Column::DispatcherId))
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
        .column((ActorDispatcher, actor_dispatcher::Column::DispatcherType))
        .column((ActorDispatcher, actor_dispatcher::Column::DispatcherId))
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
        .column(actor::Column::FragmentId)
        .column(actor_dispatcher::Column::DispatcherType)
        .column(actor_dispatcher::Column::DispatcherId)
        .table_name(cte_alias.clone())
        .to_owned();

    SelectStatement::new()
        .column(actor::Column::FragmentId)
        .column(actor_dispatcher::Column::DispatcherType)
        .column(actor_dispatcher::Column::DispatcherId)
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

#[derive(Debug, Clone)]
pub struct RescheduleWorkingSet {
    fragments: HashMap<FragmentId, fragment::Model>,
    actors: HashMap<ActorId, actor::Model>,
    actor_dispatchers: HashMap<ActorId, Vec<actor_dispatcher::Model>>,
}

impl CatalogController {
    pub async fn resolve_working_set_for_reschedule<C>(
        &self,
        txn: &C,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<RescheduleWorkingSet>
    where
        C: ConnectionTrait,
    {
        // NO_SHUFFLE related multi-layer upstream fragments
        let no_shuffle_related_fragment_ids = {
            let query = construct_no_shuffle_traverse_query(fragment_ids.clone());
            let (sql, values) = query.build_any(&*txn.get_database_backend().get_query_builder());

            txn.query_all(Statement::from_sql_and_values(
                txn.get_database_backend(),
                sql,
                values,
            ))
            .await?
            .into_iter()
            .map(|res| {
                // res.try_get_many_by_index()
                // let fragment_id: FragmentId = res.try_get_by(0).unwrap();
                // let dispatcher_type: DispatcherType = res.try_get_by(1).unwrap();
                // let dispatcher_id: FragmentId = res.try_get_by(2).unwrap();
                let ts: (FragmentId, DispatcherType, FragmentId) =
                    res.try_get_many_by_index().unwrap();

                ts
            })
            .collect_vec()
        };

        // single-layer upstream fragment ids
        let upstream_fragments: Vec<(FragmentId, DispatcherType, FragmentId)> = Actor::find()
            .select_only()
            .column(actor::Column::FragmentId)
            .column(actor_dispatcher::Column::DispatcherType)
            .column(actor_dispatcher::Column::DispatcherId)
            .distinct()
            .join(JoinType::InnerJoin, actor::Relation::ActorDispatcher.def())
            .filter(actor_dispatcher::Column::DispatcherId.is_in(fragment_ids.clone()))
            .into_tuple()
            .all(txn)
            .await?;

        // single-layer downstream fragment ids
        let downstream_fragments: Vec<(FragmentId, DispatcherType, FragmentId)> = Actor::find()
            .select_only()
            .column(actor_dispatcher::Column::DispatcherId)
            .column(actor_dispatcher::Column::DispatcherType)
            .column(actor_dispatcher::Column::DispatcherId)
            .distinct()
            .join(JoinType::InnerJoin, actor::Relation::ActorDispatcher.def())
            .filter(actor::Column::FragmentId.is_in(fragment_ids.clone()))
            .into_tuple()
            .all(txn)
            .await?;

        let all_fragment_relations: HashSet<_> = no_shuffle_related_fragment_ids
            .into_iter()
            .chain(upstream_fragments.into_iter())
            .chain(downstream_fragments.into_iter())
            .collect();

        let all_fragment_ids: HashSet<_> = all_fragment_relations
            .iter()
            .flat_map(|(src, _, dst)| [*src, *dst])
            .chain(fragment_ids.clone().into_iter())
            .collect();

        let fragments: Vec<_> = Fragment::find()
            .filter(fragment::Column::FragmentId.is_in(all_fragment_ids.clone()))
            .all(txn)
            .await?;

        let actor_and_dispatchers: Vec<(_, _)> = Actor::find()
            .filter(actor::Column::FragmentId.is_in(all_fragment_ids))
            .find_with_related(ActorDispatcher)
            .all(txn)
            .await?;

        let mut actors = HashMap::with_capacity(actor_and_dispatchers.len());
        let mut actor_dispatchers = HashMap::with_capacity(actor_and_dispatchers.len());

        for (actor, dispatchers) in actor_and_dispatchers {
            let actor_id = actor.actor_id;
            actors.insert(actor_id, actor);
            actor_dispatchers.insert(actor_id, dispatchers);
        }

        let fragments = fragments
            .into_iter()
            .map(|fragment| (fragment.fragment_id, fragment))
            .collect();

        Ok(RescheduleWorkingSet {
            fragments,
            actors,
            actor_dispatchers,
        })
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

        let working_set = mgr
            .resolve_working_set_for_reschedule(&txn, vec![8, 7, 6])
            .await
            .unwrap();

        println!("working set {:#?}", working_set);

        Ok(())
    }
}
