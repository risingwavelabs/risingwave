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
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash;
use risingwave_meta_model_migration::{
    Alias, CommonTableExpression, Expr, IntoColumnRef, QueryStatementBuilder, SelectStatement,
    UnionType, WithClause, WithQuery,
};
use risingwave_meta_model_v2::actor::ActorStatus;
use risingwave_meta_model_v2::actor_dispatcher::DispatcherType;
use risingwave_meta_model_v2::fragment::DistributionType;
use risingwave_meta_model_v2::prelude::{Actor, ActorDispatcher, Fragment, StreamingJob};
use risingwave_meta_model_v2::{
    actor, actor_dispatcher, fragment, streaming_job, ActorId, ActorMapping, ActorUpstreamActors,
    ConnectorSplits, FragmentId, I32Array, ObjectId, VnodeBitmap,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbErr, DerivePartialModel, EntityTrait, FromQueryResult,
    JoinType, QueryFilter, QuerySelect, RelationTrait, Statement, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::{model, MetaError, MetaResult};

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
/// assert_eq!(query.to_string(MysqlQueryBuilder), r#"WITH RECURSIVE `shuffle_deps` (`fragment_id`, `dispatcher_type`, `dispatcher_id`) AS (SELECT DISTINCT `actor`.`fragment_id`, `actor_dispatcher`.`dispatcher_type`, `actor_dispatcher`.`dispatcher_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE' AND `actor_dispatcher`.`dispatcher_id` IN (2, 3) UNION ALL (SELECT `actor`.`fragment_id`, `actor_dispatcher`.`dispatcher_type`, `actor_dispatcher`.`dispatcher_id` FROM `actor` INNER JOIN `actor_dispatcher` ON `actor`.`actor_id` = `actor_dispatcher`.`actor_id` INNER JOIN `shuffle_deps` ON `shuffle_deps`.`fragment_id` = `actor_dispatcher`.`dispatcher_id` WHERE `actor_dispatcher`.`dispatcher_type` = 'NO_SHUFFLE')) SELECT DISTINCT `fragment_id`, `dispatcher_type`, `dispatcher_id` FROM `shuffle_deps`"#);
/// assert_eq!(query.to_string(PostgresQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id", "dispatcher_type", "dispatcher_id") AS (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL (SELECT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE')) SELECT DISTINCT "fragment_id", "dispatcher_type", "dispatcher_id" FROM "shuffle_deps""#);
/// assert_eq!(query.to_string(SqliteQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("fragment_id", "dispatcher_type", "dispatcher_id") AS (SELECT DISTINCT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE' AND "actor_dispatcher"."dispatcher_id" IN (2, 3) UNION ALL SELECT "actor"."fragment_id", "actor_dispatcher"."dispatcher_type", "actor_dispatcher"."dispatcher_id" FROM "actor" INNER JOIN "actor_dispatcher" ON "actor"."actor_id" = "actor_dispatcher"."actor_id" INNER JOIN "shuffle_deps" ON "shuffle_deps"."fragment_id" = "actor_dispatcher"."dispatcher_id" WHERE "actor_dispatcher"."dispatcher_type" = 'NO_SHUFFLE') SELECT DISTINCT "fragment_id", "dispatcher_type", "dispatcher_id" FROM "shuffle_deps""#);
/// ```
pub fn construct_no_shuffle_upstream_traverse_query(fragment_ids: Vec<FragmentId>) -> WithQuery {
    construct_no_shuffle_traverse_query_helper(fragment_ids, NoShuffleResolveDirection::Upstream)
}

pub fn construct_no_shuffle_downstream_traverse_query(fragment_ids: Vec<FragmentId>) -> WithQuery {
    construct_no_shuffle_traverse_query_helper(fragment_ids, NoShuffleResolveDirection::Downstream)
}

enum NoShuffleResolveDirection {
    Upstream,
    Downstream,
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
        NoShuffleResolveDirection::Upstream => (
            (cte_alias.clone(), actor::Column::FragmentId).into_column_ref(),
            (ActorDispatcher, actor_dispatcher::Column::DispatcherId).into_column_ref(),
        ),
        NoShuffleResolveDirection::Downstream => (
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
        // NOTE: Uncomment me once MySQL supports DISTINCT in the recursive block of CTE.
        //.distinct()
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
    pub fragments: HashMap<FragmentId, fragment::Model>,
    pub actors: HashMap<ActorId, actor::Model>,
    pub actor_dispatchers: HashMap<ActorId, Vec<actor_dispatcher::Model>>,

    pub fragment_downstreams: HashMap<FragmentId, Vec<(FragmentId, DispatcherType)>>,
    pub fragment_upstreams: HashMap<FragmentId, Vec<(FragmentId, DispatcherType)>>,

    pub related_jobs: HashMap<ObjectId, streaming_job::Model>,
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
        .map_err(MetaError::from)?;

    Ok(result)
}

impl CatalogController {
    pub async fn resolve_working_set_for_reschedule_fragments(
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
            construct_no_shuffle_downstream_traverse_query(
                no_shuffle_related_upstream_fragment_ids
                    .iter()
                    .map(|(src, _, _)| *src)
                    .chain(fragment_ids.iter().cloned())
                    .unique()
                    .collect(),
            ),
        )
            .await?;

        // We need to identify all other types of dispatchers that are Leaves in the NO_SHUFFLE dependency tree.
        let extended_fragment_ids: HashSet<_> = no_shuffle_related_upstream_fragment_ids
            .iter()
            .chain(no_shuffle_related_downstream_fragment_ids.iter())
            .flat_map(|(src, _, dst)| [*src, *dst])
            .chain(fragment_ids.iter().cloned())
            .collect();

        let query = Actor::find()
            .select_only()
            .column(actor::Column::FragmentId)
            .column(actor_dispatcher::Column::DispatcherType)
            .column(actor_dispatcher::Column::DispatcherId)
            .distinct()
            .join(JoinType::InnerJoin, actor::Relation::ActorDispatcher.def());

        // single-layer upstream fragment ids
        let upstream_fragments: Vec<(FragmentId, DispatcherType, FragmentId)> = query
            .clone()
            .filter(actor_dispatcher::Column::DispatcherId.is_in(extended_fragment_ids.clone()))
            .into_tuple()
            .all(txn)
            .await?;

        // single-layer downstream fragment ids
        let downstream_fragments: Vec<(FragmentId, DispatcherType, FragmentId)> = query
            .clone()
            .filter(actor::Column::FragmentId.is_in(extended_fragment_ids.clone()))
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
            .chain(extended_fragment_ids.into_iter())
            .collect();

        let fragments: Vec<_> = Fragment::find()
            .filter(fragment::Column::FragmentId.is_in(all_fragment_ids.clone()))
            .all(txn)
            .await?;

        let actor_and_dispatchers: Vec<(_, _)> = Actor::find()
            .filter(actor::Column::FragmentId.is_in(all_fragment_ids.clone()))
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

        let fragments: HashMap<FragmentId, _> = fragments
            .into_iter()
            .map(|fragment| (fragment.fragment_id, fragment))
            .collect();

        let related_job_ids: HashSet<_> =
            fragments.values().map(|fragment| fragment.job_id).collect();

        let related_jobs = StreamingJob::find()
            .filter(streaming_job::Column::JobId.is_in(related_job_ids))
            .all(txn)
            .await?;

        let related_jobs = related_jobs
            .into_iter()
            .map(|job| (job.job_id, job))
            .collect();

        Ok(RescheduleWorkingSet {
            fragments,
            actors,
            actor_dispatchers,
            fragment_downstreams,
            fragment_upstreams,
            related_jobs,
        })
    }
}

macro_rules! perform_check_in_loop {
    ($flag:expr, $condition:expr, $message:expr) => {
        if !$condition {
            tracing::error!("Integrity check failed: {}", $message);
            $flag = true;
            continue;
        }
    };
}

impl CatalogController {
    pub async fn integrity_check(&self) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        #[derive(Clone, DerivePartialModel, FromQueryResult)]
        #[sea_orm(entity = "ActorDispatcher")]
        pub struct PartialActorDispatcher {
            pub id: i32,
            pub actor_id: ActorId,
            pub dispatcher_type: DispatcherType,
            pub hash_mapping: Option<ActorMapping>,
            pub dispatcher_id: FragmentId,
            pub downstream_actor_ids: I32Array,
        }

        #[derive(Clone, DerivePartialModel, FromQueryResult)]
        #[sea_orm(entity = "Fragment")]
        pub struct PartialFragment {
            pub fragment_id: FragmentId,
            pub job_id: ObjectId,
            pub distribution_type: DistributionType,
            pub upstream_fragment_id: I32Array,
        }

        #[derive(Clone, DerivePartialModel, FromQueryResult)]
        #[sea_orm(entity = "Actor")]
        pub struct PartialActor {
            pub actor_id: ActorId,
            pub fragment_id: FragmentId,
            pub status: ActorStatus,
            pub splits: Option<ConnectorSplits>,
            pub upstream_actor_ids: ActorUpstreamActors,
            pub vnode_bitmap: Option<VnodeBitmap>,
        }

        let mut flag = false;

        let fragments: Vec<PartialFragment> =
            Fragment::find().into_partial_model().all(&txn).await?;

        let fragment_map: HashMap<_, _> = fragments
            .into_iter()
            .map(|fragment| (fragment.fragment_id, fragment))
            .collect();

        let actors: Vec<PartialActor> = Actor::find().into_partial_model().all(&txn).await?;

        let mut fragment_actors = HashMap::new();
        for actor in &actors {
            fragment_actors
                .entry(actor.fragment_id)
                .or_insert(HashSet::new())
                .insert(actor.actor_id);
        }

        let actor_map: HashMap<_, _> = actors
            .into_iter()
            .map(|actor| (actor.actor_id, actor))
            .collect();

        let actor_dispatchers: Vec<PartialActorDispatcher> = ActorDispatcher::find()
            .into_partial_model()
            .all(&txn)
            .await?;

        let mut discovered_upstream_fragments = HashMap::new();
        let mut discovered_upstream_actors = HashMap::new();

        for PartialActorDispatcher {
            id,
            actor_id,
            dispatcher_type,
            hash_mapping,
            dispatcher_id,
            downstream_actor_ids,
        } in &actor_dispatchers
        {
            perform_check_in_loop!(
                flag,
                actor_map.contains_key(actor_id),
                format!(
                    "ActorDispatcher {} has actor_id {} which does not exist",
                    id, actor_id
                )
            );

            let actor = &actor_map[actor_id];

            perform_check_in_loop!(
                flag,
                fragment_map.contains_key(dispatcher_id),
                format!(
                    "ActorDispatcher {} has dispatcher_id {} which does not exist",
                    id, dispatcher_id
                )
            );

            discovered_upstream_fragments
                .entry(*dispatcher_id)
                .or_insert(HashSet::new())
                .insert(actor.fragment_id);

            let downstream_fragment = &fragment_map[dispatcher_id];

            perform_check_in_loop!(
                flag,
                downstream_fragment.upstream_fragment_id.inner_ref().contains(&actor.fragment_id),
                format!(
                    "ActorDispatcher {} has downstream fragment {} which does not have upstream fragment {}",
                    id, dispatcher_id, actor.fragment_id
                )
            );

            perform_check_in_loop!(
                flag,
                fragment_actors.contains_key(&dispatcher_id),
                format!(
                    "ActorDispatcher {id} has downstream fragment {dispatcher_id} which has no actors",
                )
            );

            let dispatcher_downstream_actor_ids: HashSet<_> =
                downstream_actor_ids.inner_ref().iter().cloned().collect();

            let target_fragment_actor_ids = &fragment_actors[&dispatcher_id];

            for dispatcher_downstream_actor_id in &dispatcher_downstream_actor_ids {
                perform_check_in_loop!(
                    flag,
                    actor_map.contains_key(&dispatcher_downstream_actor_id),
                    format!(
                        "ActorDispatcher {id} has downstream_actor_id {dispatcher_downstream_actor_id} which does not exist",
                    )
                );

                let actor_fragment_id = actor.fragment_id;

                perform_check_in_loop!(
                    flag,
                    actor_map[dispatcher_downstream_actor_id].upstream_actor_ids.inner_ref().contains_key(&actor.fragment_id),
                    format!(
                        "ActorDispatcher {id} has downstream_actor_id {dispatcher_downstream_actor_id} which does not have fragment_id {actor_fragment_id} in upstream_actor_id",
                    )
                );

                discovered_upstream_actors
                    .entry(*dispatcher_downstream_actor_id)
                    .or_insert(HashSet::new())
                    .insert(actor.actor_id);
            }

            match dispatcher_type {
                DispatcherType::NoShuffle => {}
                _ => {
                    perform_check_in_loop!(
                        flag,
                        &dispatcher_downstream_actor_ids == target_fragment_actor_ids,
                        format!(
                            "ActorDispatcher {id} has downstream fragment {dispatcher_id} which has different actors: {dispatcher_downstream_actor_ids:?} != {target_fragment_actor_ids:?}",
                        )
                    );
                }
            }

            match dispatcher_type {
                DispatcherType::Hash => {
                    perform_check_in_loop!(
                        flag,
                        hash_mapping.is_some(),
                        format!(
                            "ActorDispatcher {id} has no hash_mapping set for {dispatcher_type:?}",
                        )
                    );
                }
                _ => {
                    perform_check_in_loop!(
                        flag,
                        hash_mapping.is_none(),
                        format!(
                            "ActorDispatcher {id} has hash_mapping set for {dispatcher_type:?}"
                        )
                    );
                }
            }

            match dispatcher_type {
                DispatcherType::Simple | DispatcherType::NoShuffle => {
                    perform_check_in_loop!(
                        flag,
                        dispatcher_downstream_actor_ids.len() == 1,
                        format!(
                            "ActorDispatcher {id} has more than one downstream_actor_ids for {dispatcher_type:?}",
                        )
                    );
                }
                _ => {}
            }

            match dispatcher_type {
                DispatcherType::Hash => {
                    let mapping = hash::ActorMapping::from_protobuf(
                        &hash_mapping.as_ref().unwrap().to_protobuf(),
                    );

                    let mapping_actors: HashSet<_> =
                        mapping.iter().map(|actor_id| actor_id as ActorId).collect();

                    perform_check_in_loop!(
                        flag,
                        &mapping_actors == target_fragment_actor_ids,
                        format!(
                            "ActorDispatcher {id} has downstream fragment {dispatcher_id} which has different actors: {mapping_actors:?} != {target_fragment_actor_ids:?}",
                        )
                    );

                    // actors only from hash distribution fragment can have hash mapping
                    if let DistributionType::Hash = downstream_fragment.distribution_type {
                        let mut downstream_bitmaps = HashMap::new();

                        for downstream_actor in target_fragment_actor_ids {
                            // todo, check map before
                            let bitmap = Bitmap::from(
                                &actor_map[downstream_actor]
                                    .vnode_bitmap
                                    .as_ref()
                                    .unwrap()
                                    .to_protobuf(),
                            );

                            downstream_bitmaps.insert(*downstream_actor as hash::ActorId, bitmap);
                        }

                        perform_check_in_loop!(
                            flag,
                            mapping.to_bitmaps() == downstream_bitmaps,
                            format!(
                                "ActorDispatcher {id} has hash downstream fragment {dispatcher_id} which has different bitmaps: {mapping:?} != {downstream_bitmaps:?}"
                            )
                        );
                    }
                }

                DispatcherType::Simple => {
                    perform_check_in_loop!(
                        flag,
                        target_fragment_actor_ids.len() == 1,
                        format!(
                            "ActorDispatcher {id} has more than one actors in downstream fragment {dispatcher_id} for {dispatcher_type:?}",
                        )
                    );
                }

                DispatcherType::NoShuffle => {
                    let downstream_actor_id =
                        dispatcher_downstream_actor_ids.iter().next().unwrap();
                    let downstream_actor = &actor_map[downstream_actor_id];

                    perform_check_in_loop!(
                        flag,
                        actor.vnode_bitmap == downstream_actor.vnode_bitmap,
                        format!(
                            "ActorDispatcher {id} has different vnode_bitmap with downstream_actor_id {downstream_actor_id} for {dispatcher_type:?}",
                        )
                    );
                }

                DispatcherType::Broadcast => {}
            }
        }

        for (fragment_id, fragment) in &fragment_map {
            // todo, check job id

            for upstream_fragment_id in fragment.upstream_fragment_id.inner_ref() {
                perform_check_in_loop!(
                    flag,
                    fragment_map.contains_key(upstream_fragment_id),
                    format!(
                        "Fragment {} has upstream fragment {} which does not exist",
                        fragment_id, upstream_fragment_id
                    )
                );
            }
        }

        // todo, fragment.upstream_fragment = actor.dispatcher to fragment

        let _ = discovered_upstream_fragments;
        // let _ = discovered_upstream_actors;

        if flag {
            return Err(MetaError::integrity_check_failed());
        }

        Ok(())
    }
}
