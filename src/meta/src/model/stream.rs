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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::{AddAssign, Deref};

use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask, TableId};
use risingwave_common::hash::{
    ActorAlignmentId, IsSingleton, VirtualNode, VnodeCount, VnodeCountCompat,
};
use risingwave_common::id::JobId;
use risingwave_common::util::stream_graph_visitor::{self, visit_stream_node_body};
use risingwave_meta_model::{DispatcherType, SourceId, StreamingParallelism, WorkerId};
use risingwave_pb::catalog::Table;
use risingwave_pb::common::{ActorInfo, PbActorLocation};
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::meta::table_fragments::{ActorStatus, PbFragment, State};
use risingwave_pb::meta::table_parallelism::{
    FixedParallelism, Parallelism, PbAdaptiveParallelism, PbCustomParallelism, PbFixedParallelism,
    PbParallelism,
};
use risingwave_pb::meta::{PbTableFragments, PbTableParallelism};
use risingwave_pb::plan_common::PbExprContext;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, Dispatcher, PbDispatchOutputMapping, PbDispatcher, PbStreamActor,
    PbStreamContext, StreamNode,
};

use super::{ActorId, FragmentId};

/// The parallelism for a `TableFragments`.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableParallelism {
    /// This is when the system decides the parallelism, based on the available worker parallelisms.
    Adaptive,
    /// We set this when the `TableFragments` parallelism is changed.
    /// All fragments which are part of the `TableFragment` will have the same parallelism as this.
    Fixed(usize),
    /// We set this when the individual parallelisms of the `Fragments`
    /// can differ within a `TableFragments`.
    /// This is set for `risectl`, since it has a low-level interface,
    /// scale individual `Fragments` within `TableFragments`.
    /// When that happens, the `TableFragments` no longer has a consistent
    /// parallelism, so we set this to indicate that.
    Custom,
}

impl From<PbTableParallelism> for TableParallelism {
    fn from(value: PbTableParallelism) -> Self {
        use Parallelism::*;
        match &value.parallelism {
            Some(Fixed(FixedParallelism { parallelism: n })) => Self::Fixed(*n as usize),
            Some(Adaptive(_)) | Some(Auto(_)) => Self::Adaptive,
            Some(Custom(_)) => Self::Custom,
            _ => unreachable!(),
        }
    }
}

impl From<TableParallelism> for PbTableParallelism {
    fn from(value: TableParallelism) -> Self {
        use TableParallelism::*;

        let parallelism = match value {
            Adaptive => PbParallelism::Adaptive(PbAdaptiveParallelism {}),
            Fixed(n) => PbParallelism::Fixed(PbFixedParallelism {
                parallelism: n as u32,
            }),
            Custom => PbParallelism::Custom(PbCustomParallelism {}),
        };

        Self {
            parallelism: Some(parallelism),
        }
    }
}

impl From<StreamingParallelism> for TableParallelism {
    fn from(value: StreamingParallelism) -> Self {
        match value {
            StreamingParallelism::Adaptive => TableParallelism::Adaptive,
            StreamingParallelism::Fixed(n) => TableParallelism::Fixed(n),
            StreamingParallelism::Custom => TableParallelism::Custom,
        }
    }
}

impl From<TableParallelism> for StreamingParallelism {
    fn from(value: TableParallelism) -> Self {
        match value {
            TableParallelism::Adaptive => StreamingParallelism::Adaptive,
            TableParallelism::Fixed(n) => StreamingParallelism::Fixed(n),
            TableParallelism::Custom => StreamingParallelism::Custom,
        }
    }
}

pub type ActorUpstreams = BTreeMap<FragmentId, HashMap<ActorId, ActorInfo>>;
pub type StreamActorWithDispatchers = (StreamActor, Vec<PbDispatcher>);
pub type StreamActorWithUpDownstreams = (StreamActor, ActorUpstreams, Vec<PbDispatcher>);
pub type FragmentActorDispatchers = HashMap<FragmentId, HashMap<ActorId, Vec<PbDispatcher>>>;

pub type FragmentDownstreamRelation = HashMap<FragmentId, Vec<DownstreamFragmentRelation>>;
/// downstream `fragment_id` -> original upstream `fragment_id` -> new upstream `fragment_id`
pub type FragmentReplaceUpstream = HashMap<FragmentId, HashMap<FragmentId, FragmentId>>;
/// The newly added no-shuffle actor dispatcher from upstream fragment to downstream fragment
/// upstream `fragment_id` -> downstream `fragment_id` -> upstream `actor_id` -> downstream `actor_id`
pub type FragmentNewNoShuffle = HashMap<FragmentId, HashMap<FragmentId, HashMap<ActorId, ActorId>>>;

#[derive(Debug, Clone)]
pub struct DownstreamFragmentRelation {
    pub downstream_fragment_id: FragmentId,
    pub dispatcher_type: DispatcherType,
    pub dist_key_indices: Vec<u32>,
    pub output_mapping: PbDispatchOutputMapping,
}

impl From<(FragmentId, DispatchStrategy)> for DownstreamFragmentRelation {
    fn from((fragment_id, dispatch): (FragmentId, DispatchStrategy)) -> Self {
        Self {
            downstream_fragment_id: fragment_id,
            dispatcher_type: dispatch.get_type().unwrap().into(),
            dist_key_indices: dispatch.dist_key_indices,
            output_mapping: dispatch.output_mapping.unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamJobFragmentsToCreate {
    pub inner: StreamJobFragments,
    pub downstreams: FragmentDownstreamRelation,
}

impl Deref for StreamJobFragmentsToCreate {
    type Target = StreamJobFragments;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone, Debug)]
pub struct StreamActor {
    pub actor_id: ActorId,
    pub fragment_id: FragmentId,
    pub vnode_bitmap: Option<Bitmap>,
    pub mview_definition: String,
    pub expr_context: Option<PbExprContext>,
}

impl StreamActor {
    fn to_protobuf(&self, dispatchers: impl Iterator<Item = Dispatcher>) -> PbStreamActor {
        PbStreamActor {
            actor_id: self.actor_id,
            fragment_id: self.fragment_id,
            dispatcher: dispatchers.collect(),
            vnode_bitmap: self
                .vnode_bitmap
                .as_ref()
                .map(|bitmap| bitmap.to_protobuf()),
            mview_definition: self.mview_definition.clone(),
            expr_context: self.expr_context.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Fragment {
    pub fragment_id: FragmentId,
    pub fragment_type_mask: FragmentTypeMask,
    pub distribution_type: PbFragmentDistributionType,
    pub actors: Vec<StreamActor>,
    pub state_table_ids: Vec<TableId>,
    pub maybe_vnode_count: Option<u32>,
    pub nodes: StreamNode,
}

impl Fragment {
    pub fn to_protobuf(
        &self,
        upstream_fragments: impl Iterator<Item = FragmentId>,
        dispatchers: Option<&HashMap<ActorId, Vec<Dispatcher>>>,
    ) -> PbFragment {
        PbFragment {
            fragment_id: self.fragment_id,
            fragment_type_mask: self.fragment_type_mask.into(),
            distribution_type: self.distribution_type as _,
            actors: self
                .actors
                .iter()
                .map(|actor| {
                    actor.to_protobuf(
                        dispatchers
                            .and_then(|dispatchers| dispatchers.get(&(actor.actor_id as _)))
                            .into_iter()
                            .flatten()
                            .cloned(),
                    )
                })
                .collect(),
            state_table_ids: self.state_table_ids.clone(),
            upstream_fragment_ids: upstream_fragments.collect(),
            maybe_vnode_count: self.maybe_vnode_count,
            nodes: Some(self.nodes.clone()),
        }
    }
}

impl VnodeCountCompat for Fragment {
    fn vnode_count_inner(&self) -> VnodeCount {
        VnodeCount::from_protobuf(self.maybe_vnode_count, || self.is_singleton())
    }
}

impl IsSingleton for Fragment {
    fn is_singleton(&self) -> bool {
        matches!(self.distribution_type, FragmentDistributionType::Single)
    }
}

/// Fragments of a streaming job. Corresponds to [`PbTableFragments`].
/// (It was previously called `TableFragments` due to historical reasons.)
///
/// We store whole fragments in a single column family as follow:
/// `stream_job_id` => `StreamJobFragments`.
#[derive(Debug, Clone)]
pub struct StreamJobFragments {
    /// The table id.
    pub stream_job_id: JobId,

    /// The state of the table fragments.
    pub state: State,

    /// The table fragments.
    pub fragments: BTreeMap<FragmentId, Fragment>,

    /// The status of actors
    pub actor_status: BTreeMap<ActorId, ActorStatus>,

    /// The streaming context associated with this stream plan and its fragments
    pub ctx: StreamContext,

    /// The parallelism assigned to this table fragments
    pub assigned_parallelism: TableParallelism,

    /// The max parallelism specified when the streaming job was created, i.e., expected vnode count.
    ///
    /// The reason for persisting this value is mainly to check if a parallelism change (via `ALTER
    /// .. SET PARALLELISM`) is valid, so that the behavior can be consistent with the creation of
    /// the streaming job.
    ///
    /// Note that the actual vnode count, denoted by `vnode_count` in `fragments`, may be different
    /// from this value (see `StreamFragmentGraph.max_parallelism` for more details.). As a result,
    /// checking the parallelism change with this value can be inaccurate in some cases. However,
    /// when generating resizing plans, we still take the `vnode_count` of each fragment into account.
    pub max_parallelism: usize,
}

#[derive(Debug, Clone, Default)]
pub struct StreamContext {
    /// The timezone used to interpret timestamps and dates for conversion
    pub timezone: Option<String>,
}

impl StreamContext {
    pub fn to_protobuf(&self) -> PbStreamContext {
        PbStreamContext {
            timezone: self.timezone.clone().unwrap_or("".into()),
        }
    }

    pub fn to_expr_context(&self) -> PbExprContext {
        PbExprContext {
            // `self.timezone` must always be set; an invalid value is used here for debugging if it's not.
            time_zone: self.timezone.clone().unwrap_or("Empty Time Zone".into()),
            strict_mode: false,
        }
    }

    pub fn from_protobuf(prost: &PbStreamContext) -> Self {
        Self {
            timezone: if prost.get_timezone().is_empty() {
                None
            } else {
                Some(prost.get_timezone().clone())
            },
        }
    }
}

impl StreamJobFragments {
    pub fn to_protobuf(
        &self,
        fragment_upstreams: &HashMap<FragmentId, HashSet<FragmentId>>,
        fragment_dispatchers: &FragmentActorDispatchers,
    ) -> PbTableFragments {
        PbTableFragments {
            table_id: self.stream_job_id,
            state: self.state as _,
            fragments: self
                .fragments
                .iter()
                .map(|(id, fragment)| {
                    (
                        *id,
                        fragment.to_protobuf(
                            fragment_upstreams.get(id).into_iter().flatten().cloned(),
                            fragment_dispatchers.get(&(*id as _)),
                        ),
                    )
                })
                .collect(),
            actor_status: self
                .actor_status
                .iter()
                .map(|(actor_id, status)| (*actor_id, *status))
                .collect(),
            ctx: Some(self.ctx.to_protobuf()),
            parallelism: Some(self.assigned_parallelism.into()),
            node_label: "".to_owned(),
            backfill_done: true,
            max_parallelism: Some(self.max_parallelism as _),
        }
    }
}

pub type StreamJobActorsToCreate = HashMap<
    WorkerId,
    HashMap<FragmentId, (StreamNode, Vec<StreamActorWithUpDownstreams>, HashSet<u32>)>,
>;

impl StreamJobFragments {
    /// Create a new `TableFragments` with state of `Initial`, with other fields empty.
    pub fn for_test(job_id: JobId, fragments: BTreeMap<FragmentId, Fragment>) -> Self {
        Self::new(
            job_id,
            fragments,
            &BTreeMap::new(),
            StreamContext::default(),
            TableParallelism::Adaptive,
            VirtualNode::COUNT_FOR_TEST,
        )
    }

    /// Create a new `TableFragments` with state of `Initial`, recording actor locations on the given
    /// workers.
    pub fn new(
        stream_job_id: JobId,
        fragments: BTreeMap<FragmentId, Fragment>,
        actor_locations: &BTreeMap<ActorId, ActorAlignmentId>,
        ctx: StreamContext,
        table_parallelism: TableParallelism,
        max_parallelism: usize,
    ) -> Self {
        let actor_status = actor_locations
            .iter()
            .map(|(&actor_id, alignment_id)| {
                (
                    actor_id,
                    ActorStatus {
                        location: PbActorLocation::from_worker(alignment_id.worker_id()),
                    },
                )
            })
            .collect();

        Self {
            stream_job_id,
            state: State::Initial,
            fragments,
            actor_status,
            ctx,
            assigned_parallelism: table_parallelism,
            max_parallelism,
        }
    }

    pub fn fragment_ids(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.fragments.keys().cloned()
    }

    pub fn fragments(&self) -> impl Iterator<Item = &Fragment> {
        self.fragments.values()
    }

    pub fn fragment_actors(&self, fragment_id: FragmentId) -> &[StreamActor] {
        self.fragments
            .get(&fragment_id)
            .map(|f| f.actors.as_slice())
            .unwrap_or_default()
    }

    /// Returns the table id.
    pub fn stream_job_id(&self) -> JobId {
        self.stream_job_id
    }

    /// Returns the timezone of the table
    pub fn timezone(&self) -> Option<String> {
        self.ctx.timezone.clone()
    }

    /// Returns whether the table fragments is in `Created` state.
    pub fn is_created(&self) -> bool {
        self.state == State::Created
    }

    /// Returns actor ids associated with this table.
    pub fn actor_ids(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
    }

    pub fn actor_fragment_mapping(&self) -> HashMap<ActorId, FragmentId> {
        self.fragments
            .values()
            .flat_map(|fragment| {
                fragment
                    .actors
                    .iter()
                    .map(|actor| (actor.actor_id, fragment.fragment_id))
            })
            .collect()
    }

    /// Returns actors associated with this table.
    #[cfg(test)]
    pub fn actors(&self) -> Vec<StreamActor> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.clone())
            .collect()
    }

    /// Returns mview fragment ids.
    #[cfg(test)]
    pub fn mview_fragment_ids(&self) -> Vec<FragmentId> {
        self.fragments
            .values()
            .filter(move |fragment| {
                fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::Mview)
            })
            .map(|fragment| fragment.fragment_id)
            .collect()
    }

    pub fn tracking_progress_actor_ids(&self) -> Vec<(ActorId, BackfillUpstreamType)> {
        Self::tracking_progress_actor_ids_impl(self.fragments.values().map(|fragment| {
            (
                fragment.fragment_type_mask,
                fragment.actors.iter().map(|actor| actor.actor_id),
            )
        }))
    }

    /// Returns actor ids that need to be tracked when creating MV.
    pub fn tracking_progress_actor_ids_impl(
        fragments: impl IntoIterator<Item = (FragmentTypeMask, impl Iterator<Item = ActorId>)>,
    ) -> Vec<(ActorId, BackfillUpstreamType)> {
        let mut actor_ids = vec![];
        for (fragment_type_mask, actors) in fragments {
            if fragment_type_mask.contains(FragmentTypeFlag::CdcFilter) {
                // Note: CDC table job contains a StreamScan fragment (StreamCdcScan node) and a CdcFilter fragment.
                // We don't track any fragments' progress.
                return vec![];
            }
            if fragment_type_mask.contains_any([
                FragmentTypeFlag::Values,
                FragmentTypeFlag::StreamScan,
                FragmentTypeFlag::SourceScan,
                FragmentTypeFlag::LocalityProvider,
            ]) {
                actor_ids.extend(actors.map(|actor_id| {
                    (
                        actor_id,
                        BackfillUpstreamType::from_fragment_type_mask(fragment_type_mask),
                    )
                }));
            }
        }
        actor_ids
    }

    pub fn root_fragment(&self) -> Option<Fragment> {
        self.mview_fragment()
            .or_else(|| self.sink_fragment())
            .or_else(|| self.source_fragment())
    }

    /// Returns the fragment with the `Mview` type flag.
    pub fn mview_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::Mview)
            })
            .cloned()
    }

    pub fn source_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::Source)
            })
            .cloned()
    }

    pub fn sink_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| fragment.fragment_type_mask.contains(FragmentTypeFlag::Sink))
            .cloned()
    }

    /// Extract the fragments that include source executors that contains an external stream source,
    /// grouping by source id.
    pub fn stream_source_fragments(&self) -> HashMap<SourceId, BTreeSet<FragmentId>> {
        let mut source_fragments = HashMap::new();

        for fragment in self.fragments() {
            {
                if let Some(source_id) = fragment.nodes.find_stream_source() {
                    source_fragments
                        .entry(source_id)
                        .or_insert(BTreeSet::new())
                        .insert(fragment.fragment_id as FragmentId);
                }
            }
        }
        source_fragments
    }

    pub fn source_backfill_fragments(
        &self,
    ) -> HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>> {
        Self::source_backfill_fragments_impl(
            self.fragments
                .iter()
                .map(|(fragment_id, fragment)| (*fragment_id, &fragment.nodes)),
        )
    }

    /// Returns (`source_id`, -> (`source_backfill_fragment_id`, `upstream_source_fragment_id`)).
    ///
    /// Note: the fragment `source_backfill_fragment_id` may actually have multiple upstream fragments,
    /// but only one of them is the upstream source fragment, which is what we return.
    pub fn source_backfill_fragments_impl(
        fragments: impl Iterator<Item = (FragmentId, &StreamNode)>,
    ) -> HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>> {
        let mut source_backfill_fragments = HashMap::new();

        for (fragment_id, fragment_node) in fragments {
            {
                if let Some((source_id, upstream_source_fragment_id)) =
                    fragment_node.find_source_backfill()
                {
                    source_backfill_fragments
                        .entry(source_id)
                        .or_insert(BTreeSet::new())
                        .insert((fragment_id, upstream_source_fragment_id));
                }
            }
        }
        source_backfill_fragments
    }

    /// Find the table job's `Union` fragment.
    /// Panics if not found.
    pub fn union_fragment_for_table(&mut self) -> &mut Fragment {
        let mut union_fragment_id = None;
        for (fragment_id, fragment) in &self.fragments {
            {
                {
                    visit_stream_node_body(&fragment.nodes, |body| {
                        if let NodeBody::Union(_) = body {
                            if let Some(union_fragment_id) = union_fragment_id.as_mut() {
                                // The union fragment should be unique.
                                assert_eq!(*union_fragment_id, *fragment_id);
                            } else {
                                union_fragment_id = Some(*fragment_id);
                            }
                        }
                    })
                }
            }
        }

        let union_fragment_id =
            union_fragment_id.expect("fragment of placeholder merger not found");

        (self
            .fragments
            .get_mut(&union_fragment_id)
            .unwrap_or_else(|| panic!("fragment {} not found", union_fragment_id))) as _
    }

    /// Resolve dependent table
    fn resolve_dependent_table(stream_node: &StreamNode, table_ids: &mut HashMap<TableId, usize>) {
        let table_id = match stream_node.node_body.as_ref() {
            Some(NodeBody::StreamScan(stream_scan)) => Some(stream_scan.table_id),
            Some(NodeBody::StreamCdcScan(stream_scan)) => Some(stream_scan.table_id),
            _ => None,
        };
        if let Some(table_id) = table_id {
            table_ids.entry(table_id).or_default().add_assign(1);
        }

        for child in &stream_node.input {
            Self::resolve_dependent_table(child, table_ids);
        }
    }

    pub fn upstream_table_counts(&self) -> HashMap<TableId, usize> {
        Self::upstream_table_counts_impl(self.fragments.values().map(|fragment| &fragment.nodes))
    }

    /// Returns upstream table counts.
    pub fn upstream_table_counts_impl(
        fragment_nodes: impl Iterator<Item = &StreamNode>,
    ) -> HashMap<TableId, usize> {
        let mut table_ids = HashMap::new();
        fragment_nodes.for_each(|node| {
            Self::resolve_dependent_table(node, &mut table_ids);
        });

        table_ids
    }

    /// Returns actor locations group by worker id.
    pub fn worker_actor_ids(&self) -> BTreeMap<WorkerId, Vec<ActorId>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.worker_id();
            map.entry(node_id).or_insert_with(Vec::new).push(actor_id);
        }
        map
    }

    pub fn actors_to_create(
        &self,
    ) -> impl Iterator<
        Item = (
            FragmentId,
            &StreamNode,
            impl Iterator<Item = (&StreamActor, WorkerId)> + '_,
        ),
    > + '_ {
        self.fragments.values().map(move |fragment| {
            (
                fragment.fragment_id,
                &fragment.nodes,
                fragment.actors.iter().map(move |actor| {
                    let worker_id: WorkerId = self
                        .actor_status
                        .get(&actor.actor_id)
                        .expect("should exist")
                        .worker_id();
                    (actor, worker_id)
                }),
            )
        })
    }

    pub fn mv_table_id(&self) -> Option<TableId> {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.iter().copied())
            .find(|table_id| self.stream_job_id.is_mv_table_id(*table_id))
    }

    pub fn collect_tables(fragments: impl Iterator<Item = &Fragment>) -> BTreeMap<TableId, Table> {
        let mut tables = BTreeMap::new();
        for fragment in fragments {
            stream_graph_visitor::visit_stream_node_tables_inner(
                &mut fragment.nodes.clone(),
                false,
                true,
                |table, _| {
                    let table_id = table.id;
                    tables
                        .try_insert(table_id, table.clone())
                        .unwrap_or_else(|_| panic!("duplicated table id `{}`", table_id));
                },
            );
        }
        tables
    }

    /// Returns the internal table ids without the mview table.
    pub fn internal_table_ids(&self) -> Vec<TableId> {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.iter().copied())
            .filter(|&t| !self.stream_job_id.is_mv_table_id(t))
            .collect_vec()
    }

    /// Returns all internal table ids including the mview table.
    pub fn all_table_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.clone())
    }

    /// Fill the `expr_context` in `StreamActor`. Used for compatibility.
    pub fn fill_expr_context(mut self) -> Self {
        self.fragments.values_mut().for_each(|fragment| {
            fragment.actors.iter_mut().for_each(|actor| {
                if actor.expr_context.is_none() {
                    actor.expr_context = Some(self.ctx.to_expr_context());
                }
            });
        });
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillUpstreamType {
    MView,
    Values,
    Source,
    LocalityProvider,
}

impl BackfillUpstreamType {
    pub fn from_fragment_type_mask(mask: FragmentTypeMask) -> Self {
        let is_mview = mask.contains(FragmentTypeFlag::StreamScan);
        let is_values = mask.contains(FragmentTypeFlag::Values);
        let is_source = mask.contains(FragmentTypeFlag::SourceScan);
        let is_locality_provider = mask.contains(FragmentTypeFlag::LocalityProvider);

        // Note: in theory we can have multiple backfill executors in one fragment, but currently it's not possible.
        // See <https://github.com/risingwavelabs/risingwave/issues/6236>.
        debug_assert!(
            is_mview as u8 + is_values as u8 + is_source as u8 + is_locality_provider as u8 == 1,
            "a backfill fragment should either be mview, value, source, or locality provider, found {:?}",
            mask
        );

        if is_mview {
            BackfillUpstreamType::MView
        } else if is_values {
            BackfillUpstreamType::Values
        } else if is_source {
            BackfillUpstreamType::Source
        } else if is_locality_provider {
            BackfillUpstreamType::LocalityProvider
        } else {
            unreachable!("invalid fragment type mask: {:?}", mask);
        }
    }
}
