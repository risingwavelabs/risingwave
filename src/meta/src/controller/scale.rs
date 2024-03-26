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
    Alias, CommonTableExpression, Expr, IntoColumnRef, QueryStatementBuilder, SelectStatement,
    UnionType, WithClause, WithQuery,
};
use risingwave_meta_model_v2::actor_dispatcher::DispatcherType;
use risingwave_meta_model_v2::prelude::{Actor, ActorDispatcher, Fragment};
use risingwave_meta_model_v2::{
    actor, actor_dispatcher, fragment, ActorId, FragmentId, ObjectId, TableId,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbErr, EntityTrait, JoinType, QueryFilter, QueryResult,
    QuerySelect, QueryTrait, RelationTrait, Statement, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::{MetaError, MetaResult};

/// This function will construct a query using recursive cte to find `no_shuffle` upstream relation graph for target fragments.
///
/// # Examples
///
/// ```
/// use risingwave_meta::controller::scale::construct_no_shuffle_upstream_traverse_query;
/// use sea_orm::sea_query::*;
/// use sea_orm::*;
///
/// let query = construct_no_shuffle_upstream_traverse_query(vec![2, 3]);
///
/// assert_eq!(query.to_string(MysqlQueryBuilder), r#"WITH RECURSIVE `shuffle_deps` (`fragment_id`, `dispatcher_type`, `dispatcher_id`) AS (SELECT DISTINCT `actor`.`fragment_id`, `actor_dispatcher`.`dispatcher_type`, `actor_dispatcher`.`dispatcher_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE' AND `actor_dispatcher`.`dispatcher_id` IN (2, 3) UNION ALL (SELECT DISTINCT `actor`.`fragment_id`, `actor_dispatcher`.`dispatcher_type`, `actor_dispatcher`.`dispatcher_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` INNER JOIN `shuffle_deps` ON `shuffle_deps`.`fragment_id` = `actor_dispatcher`.`dispatcher_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE')) SELECT DISTINCT `fragment_id`, `dispatcher_type`, `dispatcher_id` FROM `shuffle_deps`"#);
/// assert_eq!(query.to_string(PostgresQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id", "dispatcher_type", "dispatcher_id") AS (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE')) SELECT DISTINCT "fragment_id", "dispatcher_type", "dispatcher_id" FROM "shuffle_deps""#);
/// assert_eq!(query.to_string(SqliteQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id", "dispatcher_type", "dispatcher_id") AS (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE') SELECT DISTINCT "fragment_id", "dispatcher_type", "dispatcher_id" FROM "shuffle_deps""#);
/// ```
pub fn construct_no_shuffle_upstream_traverse_query(fragment_ids: Vec<FragmentId>) -> WithQuery {
    construct_no_shuffle_traverse_query_helper(fragment_ids, NoShuffleResolveDirection::UPSTREAM)
}

pub fn construct_no_shuffle_downstream_traverse_query(fragment_ids: Vec<FragmentId>) -> WithQuery {
    construct_no_shuffle_traverse_query_helper(fragment_ids, NoShuffleResolveDirection::DOWNSTREAM)
}

enum NoShuffleResolveDirection {
    UPSTREAM,
    DOWNSTREAM,
}

fn construct_no_shuffle_traverse_query_helper(
    fragment_ids: Vec<FragmentId>,
    direction: NoShuffleResolveDirection,
) -> WithQuery {
    let cte_alias = Alias::new("shuffle_deps");

    // If we need to look upwards
    //     resolve by fragment_id -> dispatcher_id
    // and if downwards
    //     resolve by dispatcher_id -> fragment_id
    let (cte_ref_column, compared_column) = match direction {
        NoShuffleResolveDirection::UPSTREAM => (
            (cte_alias.clone(), actor::Column::FragmentId).into_column_ref(),
            (ActorDispatcher, actor_dispatcher::Column::DispatcherId).into_column_ref(),
        ),
        NoShuffleResolveDirection::DOWNSTREAM => (
            (cte_alias.clone(), actor_dispatcher::Column::DispatcherId).into_column_ref(),
            (Actor, actor::Column::FragmentId).into_column_ref(),
        ),
    };

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
        .and_where(Expr::col(compared_column.clone()).is_in(fragment_ids.clone()))
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
            Expr::col(cte_ref_column).eq(Expr::col(compared_column)),
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

    fragment_downstreams: HashMap<FragmentId, Vec<(FragmentId, DispatcherType)>>,
    fragment_upstreams: HashMap<FragmentId, Vec<(FragmentId, DispatcherType)>>,
}

async fn resolve_no_shuffle_query<C>(
    txn: &C,
    query: WithQuery,
) -> MetaResult<Vec<(FragmentId, DispatcherType, FragmentId)>>
where
    C: ConnectionTrait,
{
    let (sql, values) = query.build_any(&*txn.get_database_backend().get_query_builder());

    let result = txn
        .query_all(Statement::from_sql_and_values(
            txn.get_database_backend(),
            sql,
            values,
        ))
        .await?
        .into_iter()
        .map(|res| res.try_get_many_by_index())
        .collect::<Result<Vec<(FragmentId, DispatcherType, FragmentId)>, DbErr>>()
        .map_err(|e| MetaError::from(e))?;

    Ok(result)
}

impl CatalogController {
    pub async fn resolve_working_set_for_reschedule(
        &self,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<RescheduleWorkingSet> {
        let inner = self.inner.read().await;
        self.resolve_working_set_for_reschedule_helper(&inner.db, fragment_ids)
            .await
    }

    pub async fn resolve_working_set_for_reschedule_tables(
        &self,
        table_ids: Vec<ObjectId>,
    ) -> MetaResult<RescheduleWorkingSet> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragment_ids: Vec<FragmentId> = Fragment::find()
            .filter(fragment::Column::JobId.is_in(table_ids))
            .all(&txn)
            .await?
            .into_iter()
            .map(|fragment| fragment.fragment_id)
            .collect();

        self.resolve_working_set_for_reschedule_helper(&txn, fragment_ids)
            .await
    }

    pub async fn resolve_working_set_for_reschedule_helper<C>(
        &self,
        txn: &C,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<RescheduleWorkingSet>
    where
        C: ConnectionTrait,
    {
        // NO_SHUFFLE related multi-layer upstream fragments
        let no_shuffle_related_upstream_fragment_ids = resolve_no_shuffle_query(
            txn,
            construct_no_shuffle_upstream_traverse_query(fragment_ids.clone()),
        )
        .await?;

        // NO_SHUFFLE related multi-layer downstream fragments
        let no_shuffle_related_downstream_fragment_ids = resolve_no_shuffle_query(
            txn,
            construct_no_shuffle_downstream_traverse_query(fragment_ids.clone()),
        )
        .await?;

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

        let all_fragment_relations: HashSet<_> = no_shuffle_related_upstream_fragment_ids
            .into_iter()
            .chain(no_shuffle_related_downstream_fragment_ids.into_iter())
            .chain(upstream_fragments.into_iter())
            .chain(downstream_fragments.into_iter())
            .collect();

        let mut fragment_upstreams: HashMap<FragmentId, Vec<(FragmentId, DispatcherType)>> =
            HashMap::new();
        let mut fragment_downstreams: HashMap<FragmentId, Vec<(FragmentId, DispatcherType)>> =
            HashMap::new();

        for (src, dispatcher_type, dst) in &all_fragment_relations {
            fragment_upstreams
                .entry(*dst)
                .or_default()
                .push((*src, *dispatcher_type));
            fragment_downstreams
                .entry(*src)
                .or_default()
                .push((*dst, *dispatcher_type));
        }

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
            fragment_downstreams,
            fragment_upstreams,
        })
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use risingwave_meta_model_migration::PostgresQueryBuilder;
    use sea_orm::TransactionTrait;

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

        // println!("working set {:#?}", working_set);

        Ok(())
    }
}
