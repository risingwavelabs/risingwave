// Copyright 2025 RisingWave Labs
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
use std::ops::{BitAnd, BitOrAssign};

use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_meta_model::actor::ActorStatus;
use risingwave_meta_model::actor_dispatcher::DispatcherType;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::prelude::{
    Actor, Fragment, FragmentRelation, Sink, Source, StreamingJob, Table,
};
use risingwave_meta_model::{
    ConnectorSplits, FragmentId, ObjectId, VnodeBitmap, actor, fragment, fragment_relation, sink,
    source, streaming_job, table,
};
use risingwave_meta_model_migration::{
    Alias, CommonTableExpression, Expr, IntoColumnRef, QueryStatementBuilder, SelectStatement,
    UnionType, WithClause, WithQuery,
};
use risingwave_pb::stream_plan::PbDispatcher;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbErr, DerivePartialModel, EntityTrait, FromQueryResult,
    QueryFilter, QuerySelect, Statement, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::utils::{get_existing_job_resource_group, get_fragment_actor_dispatchers};
use crate::model::ActorId;
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
/// assert_eq!(query.to_string(MysqlQueryBuilder), r#"WITH RECURSIVE `shuffle_deps` (`source_fragment_id`, `dispatcher_type`, `target_fragment_id`) AS (SELECT DISTINCT `fragment_relation`.`source_fragment_id`, `fragment_relation`.`dispatcher_type`, `fragment_relation`.`target_fragment_id` FROM `fragment_relation` WHERE `fragment_relation`.`dispatcher_type` = 'NO_SHUFFLE' AND `fragment_relation`.`target_fragment_id` IN (2, 3) UNION ALL (SELECT `fragment_relation`.`source_fragment_id`, `fragment_relation`.`dispatcher_type`, `fragment_relation`.`target_fragment_id` FROM `fragment_relation` INNER JOIN `shuffle_deps` ON `shuffle_deps`.`source_fragment_id` = `fragment_relation`.`target_fragment_id` WHERE `fragment_relation`.`dispatcher_type` = 'NO_SHUFFLE')) SELECT DISTINCT `source_fragment_id`, `dispatcher_type`, `target_fragment_id` FROM `shuffle_deps`"#);
/// assert_eq!(query.to_string(PostgresQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("source_fragment_id", "dispatcher_type", "target_fragment_id") AS (SELECT DISTINCT "fragment_relation"."source_fragment_id", "fragment_relation"."dispatcher_type", "fragment_relation"."target_fragment_id" FROM "fragment_relation" WHERE "fragment_relation"."dispatcher_type" = 'NO_SHUFFLE' AND "fragment_relation"."target_fragment_id" IN (2, 3) UNION ALL (SELECT "fragment_relation"."source_fragment_id", "fragment_relation"."dispatcher_type", "fragment_relation"."target_fragment_id" FROM "fragment_relation" INNER JOIN "shuffle_deps" ON "shuffle_deps"."source_fragment_id" = "fragment_relation"."target_fragment_id" WHERE "fragment_relation"."dispatcher_type" = 'NO_SHUFFLE')) SELECT DISTINCT "source_fragment_id", "dispatcher_type", "target_fragment_id" FROM "shuffle_deps""#);
/// assert_eq!(query.to_string(SqliteQueryBuilder), r#"WITH RECURSIVE "shuffle_deps" ("source_fragment_id", "dispatcher_type", "target_fragment_id") AS (SELECT DISTINCT "fragment_relation"."source_fragment_id", "fragment_relation"."dispatcher_type", "fragment_relation"."target_fragment_id" FROM "fragment_relation" WHERE "fragment_relation"."dispatcher_type" = 'NO_SHUFFLE' AND "fragment_relation"."target_fragment_id" IN (2, 3) UNION ALL SELECT "fragment_relation"."source_fragment_id", "fragment_relation"."dispatcher_type", "fragment_relation"."target_fragment_id" FROM "fragment_relation" INNER JOIN "shuffle_deps" ON "shuffle_deps"."source_fragment_id" = "fragment_relation"."target_fragment_id" WHERE "fragment_relation"."dispatcher_type" = 'NO_SHUFFLE') SELECT DISTINCT "source_fragment_id", "dispatcher_type", "target_fragment_id" FROM "shuffle_deps""#);
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
    //     resolve by upstream fragment_id -> downstream fragment_id
    // and if downwards
    //     resolve by downstream fragment_id -> upstream fragment_id
    let (cte_ref_column, compared_column) = match direction {
        NoShuffleResolveDirection::Upstream => (
            (
                cte_alias.clone(),
                fragment_relation::Column::SourceFragmentId,
            )
                .into_column_ref(),
            (
                FragmentRelation,
                fragment_relation::Column::TargetFragmentId,
            )
                .into_column_ref(),
        ),
        NoShuffleResolveDirection::Downstream => (
            (
                cte_alias.clone(),
                fragment_relation::Column::TargetFragmentId,
            )
                .into_column_ref(),
            (
                FragmentRelation,
                fragment_relation::Column::SourceFragmentId,
            )
                .into_column_ref(),
        ),
    };

    let mut base_query = SelectStatement::new()
        .column((
            FragmentRelation,
            fragment_relation::Column::SourceFragmentId,
        ))
        .column((FragmentRelation, fragment_relation::Column::DispatcherType))
        .column((
            FragmentRelation,
            fragment_relation::Column::TargetFragmentId,
        ))
        .distinct()
        .from(FragmentRelation)
        .and_where(
            Expr::col((FragmentRelation, fragment_relation::Column::DispatcherType))
                .eq(DispatcherType::NoShuffle),
        )
        .and_where(Expr::col(compared_column.clone()).is_in(fragment_ids.clone()))
        .to_owned();

    let cte_referencing = SelectStatement::new()
        .column((
            FragmentRelation,
            fragment_relation::Column::SourceFragmentId,
        ))
        .column((FragmentRelation, fragment_relation::Column::DispatcherType))
        .column((
            FragmentRelation,
            fragment_relation::Column::TargetFragmentId,
        ))
        // NOTE: Uncomment me once MySQL supports DISTINCT in the recursive block of CTE.
        //.distinct()
        .from(FragmentRelation)
        .inner_join(
            cte_alias.clone(),
            Expr::col(cte_ref_column).eq(Expr::col(compared_column)),
        )
        .and_where(
            Expr::col((FragmentRelation, fragment_relation::Column::DispatcherType))
                .eq(DispatcherType::NoShuffle),
        )
        .to_owned();

    let common_table_expr = CommonTableExpression::new()
        .query(base_query.union(UnionType::All, cte_referencing).to_owned())
        .column(fragment_relation::Column::SourceFragmentId)
        .column(fragment_relation::Column::DispatcherType)
        .column(fragment_relation::Column::TargetFragmentId)
        .table_name(cte_alias.clone())
        .to_owned();

    SelectStatement::new()
        .column(fragment_relation::Column::SourceFragmentId)
        .column(fragment_relation::Column::DispatcherType)
        .column(fragment_relation::Column::TargetFragmentId)
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
    pub actor_dispatchers: HashMap<ActorId, Vec<PbDispatcher>>,

    pub fragment_downstreams: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>>,
    pub fragment_upstreams: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>>,

    pub job_resource_groups: HashMap<ObjectId, String>,
    pub related_jobs: HashMap<ObjectId, (streaming_job::Model, String)>,
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

pub(crate) async fn resolve_streaming_job_definition<C>(
    txn: &C,
    job_ids: &HashSet<ObjectId>,
) -> MetaResult<HashMap<ObjectId, String>>
where
    C: ConnectionTrait,
{
    let job_ids = job_ids.iter().cloned().collect_vec();

    // including table, materialized view, index
    let common_job_definitions: Vec<(ObjectId, String)> = Table::find()
        .select_only()
        .columns([
            table::Column::TableId,
            #[cfg(not(debug_assertions))]
            table::Column::Name,
            #[cfg(debug_assertions)]
            table::Column::Definition,
        ])
        .filter(table::Column::TableId.is_in(job_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let sink_definitions: Vec<(ObjectId, String)> = Sink::find()
        .select_only()
        .columns([
            sink::Column::SinkId,
            #[cfg(not(debug_assertions))]
            sink::Column::Name,
            #[cfg(debug_assertions)]
            sink::Column::Definition,
        ])
        .filter(sink::Column::SinkId.is_in(job_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let source_definitions: Vec<(ObjectId, String)> = Source::find()
        .select_only()
        .columns([
            source::Column::SourceId,
            #[cfg(not(debug_assertions))]
            source::Column::Name,
            #[cfg(debug_assertions)]
            source::Column::Definition,
        ])
        .filter(source::Column::SourceId.is_in(job_ids.clone()))
        .into_tuple()
        .all(txn)
        .await?;

    let definitions: HashMap<ObjectId, String> = common_job_definitions
        .into_iter()
        .chain(sink_definitions.into_iter())
        .chain(source_definitions.into_iter())
        .collect();

    Ok(definitions)
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

        let query = FragmentRelation::find()
            .select_only()
            .column(fragment_relation::Column::SourceFragmentId)
            .column(fragment_relation::Column::DispatcherType)
            .column(fragment_relation::Column::TargetFragmentId)
            .distinct();

        // single-layer upstream fragment ids
        let upstream_fragments: Vec<(FragmentId, DispatcherType, FragmentId)> = query
            .clone()
            .filter(
                fragment_relation::Column::TargetFragmentId.is_in(extended_fragment_ids.clone()),
            )
            .into_tuple()
            .all(txn)
            .await?;

        // single-layer downstream fragment ids
        let downstream_fragments: Vec<(FragmentId, DispatcherType, FragmentId)> = query
            .clone()
            .filter(
                fragment_relation::Column::SourceFragmentId.is_in(extended_fragment_ids.clone()),
            )
            .into_tuple()
            .all(txn)
            .await?;

        let all_fragment_relations: HashSet<_> = no_shuffle_related_upstream_fragment_ids
            .into_iter()
            .chain(no_shuffle_related_downstream_fragment_ids.into_iter())
            .chain(upstream_fragments.into_iter())
            .chain(downstream_fragments.into_iter())
            .collect();

        let mut fragment_upstreams: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>> =
            HashMap::new();
        let mut fragment_downstreams: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>> =
            HashMap::new();

        for (src, dispatcher_type, dst) in &all_fragment_relations {
            fragment_upstreams
                .entry(*dst)
                .or_default()
                .insert(*src, *dispatcher_type);
            fragment_downstreams
                .entry(*src)
                .or_default()
                .insert(*dst, *dispatcher_type);
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

        let actors: Vec<_> = Actor::find()
            .filter(actor::Column::FragmentId.is_in(all_fragment_ids.clone()))
            .all(txn)
            .await?;

        let actors: HashMap<_, _> = actors
            .into_iter()
            .map(|actor| (actor.actor_id as _, actor))
            .collect();

        let fragments: HashMap<FragmentId, _> = fragments
            .into_iter()
            .map(|fragment| (fragment.fragment_id, fragment))
            .collect();

        let related_job_ids: HashSet<_> =
            fragments.values().map(|fragment| fragment.job_id).collect();

        let mut job_resource_groups = HashMap::new();
        for &job_id in &related_job_ids {
            let resource_group = get_existing_job_resource_group(txn, job_id).await?;
            job_resource_groups.insert(job_id, resource_group);
        }

        let related_job_definitions =
            resolve_streaming_job_definition(txn, &related_job_ids).await?;

        let related_jobs = StreamingJob::find()
            .filter(streaming_job::Column::JobId.is_in(related_job_ids))
            .all(txn)
            .await?;

        let related_jobs = related_jobs
            .into_iter()
            .map(|job| {
                let job_id = job.job_id;
                (
                    job_id,
                    (
                        job,
                        related_job_definitions
                            .get(&job_id)
                            .cloned()
                            .unwrap_or("".to_owned()),
                    ),
                )
            })
            .collect();

        let fragment_actor_dispatchers = get_fragment_actor_dispatchers(
            txn,
            fragments
                .keys()
                .map(|fragment_id| *fragment_id as _)
                .collect(),
        )
        .await?;

        Ok(RescheduleWorkingSet {
            fragments,
            actors,
            actor_dispatchers: fragment_actor_dispatchers.into_values().flatten().collect(),
            fragment_downstreams,
            fragment_upstreams,
            job_resource_groups,
            related_jobs,
        })
    }
}

macro_rules! crit_check_in_loop {
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
        Self::graph_check(&txn).await
    }

    // Perform integrity checks on the Actor, ActorDispatcher and Fragment tables.
    pub async fn graph_check<C>(txn: &C) -> MetaResult<()>
    where
        C: ConnectionTrait,
    {
        #[derive(Clone, DerivePartialModel, FromQueryResult)]
        #[sea_orm(entity = "Fragment")]
        pub struct PartialFragment {
            pub fragment_id: FragmentId,
            pub distribution_type: DistributionType,
            pub vnode_count: i32,
        }

        #[derive(Clone, DerivePartialModel, FromQueryResult)]
        #[sea_orm(entity = "Actor")]
        pub struct PartialActor {
            pub actor_id: risingwave_meta_model::ActorId,
            pub fragment_id: FragmentId,
            pub status: ActorStatus,
            pub splits: Option<ConnectorSplits>,
            pub vnode_bitmap: Option<VnodeBitmap>,
        }

        let mut flag = false;

        let fragments: Vec<PartialFragment> =
            Fragment::find().into_partial_model().all(txn).await?;

        let fragment_map: HashMap<_, _> = fragments
            .into_iter()
            .map(|fragment| (fragment.fragment_id, fragment))
            .collect();

        let actors: Vec<PartialActor> = Actor::find().into_partial_model().all(txn).await?;

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

        for (fragment_id, actor_ids) in &fragment_actors {
            crit_check_in_loop!(
                flag,
                fragment_map.contains_key(fragment_id),
                format!("Fragment {fragment_id} has actors {actor_ids:?} which does not exist",)
            );

            let mut split_map = HashMap::new();
            for actor_id in actor_ids {
                let actor = &actor_map[actor_id];

                if let Some(splits) = &actor.splits {
                    for split in splits.to_protobuf().splits {
                        let Ok(split_impl) = SplitImpl::try_from(&split) else {
                            continue;
                        };

                        let dup_split_actor = split_map.insert(split_impl.id(), actor_id);
                        crit_check_in_loop!(
                            flag,
                            dup_split_actor.is_none(),
                            format!(
                                "Fragment {fragment_id} actor {actor_id} has duplicate split {split:?} from actor {dup_split_actor:?}",
                            )
                        );
                    }
                }
            }

            let fragment = &fragment_map[fragment_id];

            match fragment.distribution_type {
                DistributionType::Single => {
                    crit_check_in_loop!(
                        flag,
                        actor_ids.len() == 1,
                        format!(
                            "Fragment {fragment_id} has more than one actors {actor_ids:?} for single distribution type",
                        )
                    );

                    let actor_id = actor_ids.iter().exactly_one().unwrap();
                    let actor = &actor_map[actor_id];

                    crit_check_in_loop!(
                        flag,
                        actor.vnode_bitmap.is_none(),
                        format!(
                            "Fragment {fragment_id} actor {actor_id} has vnode_bitmap set for single distribution type",
                        )
                    );
                }
                DistributionType::Hash => {
                    crit_check_in_loop!(
                        flag,
                        !actor_ids.is_empty(),
                        format!(
                            "Fragment {fragment_id} has less than one actors {actor_ids:?} for hash distribution type",
                        )
                    );

                    let fragment_vnode_count = fragment.vnode_count as usize;

                    let mut result_bitmap = Bitmap::zeros(fragment_vnode_count);

                    for actor_id in actor_ids {
                        let actor = &actor_map[actor_id];

                        crit_check_in_loop!(
                            flag,
                            actor.vnode_bitmap.is_some(),
                            format!(
                                "Fragment {fragment_id} actor {actor_id} has no vnode_bitmap set for hash distribution type",
                            )
                        );

                        let bitmap =
                            Bitmap::from(actor.vnode_bitmap.as_ref().unwrap().to_protobuf());

                        crit_check_in_loop!(
                            flag,
                            result_bitmap.clone().bitand(&bitmap).count_ones() == 0,
                            format!(
                                "Fragment {fragment_id} actor {actor_id} has duplicate vnode_bitmap with other actor for hash distribution type, actor bitmap {bitmap:?}, other all bitmap {result_bitmap:?}",
                            )
                        );

                        result_bitmap.bitor_assign(&bitmap);
                    }

                    crit_check_in_loop!(
                        flag,
                        result_bitmap.all(),
                        format!(
                            "Fragment {fragment_id} has incomplete vnode_bitmap for hash distribution type",
                        )
                    );

                    let discovered_vnode_count = result_bitmap.count_ones();

                    crit_check_in_loop!(
                        flag,
                        discovered_vnode_count == fragment_vnode_count,
                        format!(
                            "Fragment {fragment_id} has different vnode_count {fragment_vnode_count} with discovered vnode count {discovered_vnode_count} for hash distribution type",
                        )
                    );
                }
            }
        }

        for PartialActor {
            actor_id, status, ..
        } in actor_map.values()
        {
            crit_check_in_loop!(
                flag,
                *status == ActorStatus::Running,
                format!("Actor {actor_id} has status {status:?} which is not Running",)
            );
        }

        if flag {
            return Err(MetaError::integrity_check_failed());
        }

        Ok(())
    }
}
