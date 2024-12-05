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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::AddAssign;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::WorkerSlotId;
use risingwave_connector::source::SplitImpl;
use risingwave_pb::common::PbActorLocation;
use risingwave_pb::meta::table_fragments::actor_status::ActorState;
use risingwave_pb::meta::table_fragments::{ActorStatus, Fragment, State};
use risingwave_pb::meta::table_parallelism::{
    FixedParallelism, Parallelism, PbAdaptiveParallelism, PbCustomParallelism, PbFixedParallelism,
    PbParallelism,
};
use risingwave_pb::meta::{PbTableFragments, PbTableParallelism};
use risingwave_pb::plan_common::PbExprContext;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    FragmentTypeFlag, PbFragmentTypeFlag, PbStreamContext, StreamActor, StreamNode,
};

use super::{ActorId, FragmentId};
use crate::manager::{SourceId, WorkerId};
use crate::model::{MetadataModel, MetadataModelResult};
use crate::stream::{build_actor_connector_splits, build_actor_split_impls, SplitAssignment};

/// Column family name for table fragments.
const TABLE_FRAGMENTS_CF_NAME: &str = "cf/table_fragments";

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

/// Fragments of a streaming job.
///
/// We store whole fragments in a single column family as follow:
/// `table_id` => `TableFragments`.
#[derive(Debug, Clone)]
pub struct TableFragments {
    /// The table id.
    table_id: TableId,

    /// The state of the table fragments.
    state: State,

    /// The table fragments.
    pub fragments: BTreeMap<FragmentId, Fragment>,

    /// The status of actors
    pub actor_status: BTreeMap<ActorId, ActorStatus>,

    /// The splits of actors
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,

    /// The streaming context associated with this stream plan and its fragments
    pub ctx: StreamContext,

    /// The parallelism assigned to this table fragments
    pub assigned_parallelism: TableParallelism,
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

impl MetadataModel for TableFragments {
    type KeyType = u32;
    type PbType = PbTableFragments;

    fn cf_name() -> String {
        TABLE_FRAGMENTS_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::PbType {
        Self::PbType {
            table_id: self.table_id.table_id(),
            state: self.state as _,
            fragments: self.fragments.clone().into_iter().collect(),
            actor_status: self.actor_status.clone().into_iter().collect(),
            actor_splits: build_actor_connector_splits(&self.actor_splits),
            ctx: Some(self.ctx.to_protobuf()),
            parallelism: Some(self.assigned_parallelism.into()),
            node_label: "".to_string(),
            backfill_done: true,
        }
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
        let ctx = StreamContext::from_protobuf(prost.get_ctx().unwrap());

        let default_parallelism = PbTableParallelism {
            parallelism: Some(Parallelism::Custom(PbCustomParallelism {})),
        };

        let state = prost.state();

        Self {
            table_id: TableId::new(prost.table_id),
            state,
            fragments: prost.fragments.into_iter().collect(),
            actor_status: prost.actor_status.into_iter().collect(),
            actor_splits: build_actor_split_impls(&prost.actor_splits),
            ctx,
            assigned_parallelism: prost.parallelism.unwrap_or(default_parallelism).into(),
        }
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.table_id.table_id())
    }
}

impl TableFragments {
    /// Create a new `TableFragments` with state of `Initial`, with other fields empty.
    pub fn for_test(table_id: TableId, fragments: BTreeMap<FragmentId, Fragment>) -> Self {
        Self::new(
            table_id,
            fragments,
            &BTreeMap::new(),
            StreamContext::default(),
            TableParallelism::Adaptive,
        )
    }

    /// Create a new `TableFragments` with state of `Initial`, with the status of actors set to
    /// `Inactive` on the given workers.
    pub fn new(
        table_id: TableId,
        fragments: BTreeMap<FragmentId, Fragment>,
        actor_locations: &BTreeMap<ActorId, WorkerSlotId>,
        ctx: StreamContext,
        table_parallelism: TableParallelism,
    ) -> Self {
        let actor_status = actor_locations
            .iter()
            .map(|(&actor_id, worker_slot_id)| {
                (
                    actor_id,
                    ActorStatus {
                        location: PbActorLocation::from_worker(worker_slot_id.worker_id()),
                        state: ActorState::Inactive as i32,
                    },
                )
            })
            .collect();

        Self {
            table_id,
            state: State::Initial,
            fragments,
            actor_status,
            actor_splits: HashMap::default(),
            ctx,
            assigned_parallelism: table_parallelism,
        }
    }

    pub fn fragment_ids(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.fragments.keys().cloned()
    }

    pub fn fragments(&self) -> impl Iterator<Item = &Fragment> {
        self.fragments.values()
    }

    /// Returns the table id.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the state of the table fragments.
    pub fn state(&self) -> State {
        self.state
    }

    /// Returns the timezone of the table
    pub fn timezone(&self) -> Option<String> {
        self.ctx.timezone.clone()
    }

    /// Returns whether the table fragments is in `Created` state.
    pub fn is_created(&self) -> bool {
        self.state == State::Created
    }

    /// Returns whether the table fragments is in `Initial` state.
    pub fn is_initial(&self) -> bool {
        self.state == State::Initial
    }

    /// Set the table ID.
    // TODO: remove this workaround for replacing table.
    pub fn set_table_id(&mut self, table_id: TableId) {
        self.table_id = table_id;
    }

    /// Set the state of the table fragments.
    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }

    /// Update state of all actors
    pub fn update_actors_state(&mut self, state: ActorState) {
        for actor_status in self.actor_status.values_mut() {
            actor_status.set_state(state);
        }
    }

    pub fn set_actor_splits_by_split_assignment(&mut self, split_assignment: SplitAssignment) {
        self.actor_splits = split_assignment.into_values().flatten().collect();
    }

    /// Returns actor ids associated with this table.
    pub fn actor_ids(&self) -> Vec<ActorId> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
            .collect()
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
    pub fn actors(&self) -> Vec<StreamActor> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.clone())
            .collect()
    }

    /// Returns the actor ids with the given fragment type.
    pub fn filter_actor_ids(
        &self,
        check_type: impl Fn(u32) -> bool + 'static,
    ) -> impl Iterator<Item = ActorId> + '_ {
        self.fragments
            .values()
            .filter(move |fragment| check_type(fragment.get_fragment_type_mask()))
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
    }

    /// Check if the fragment type mask is injectable.
    pub fn is_injectable(fragment_type_mask: u32) -> bool {
        (fragment_type_mask
            & (PbFragmentTypeFlag::Source as u32
                | PbFragmentTypeFlag::Now as u32
                | PbFragmentTypeFlag::Values as u32
                | PbFragmentTypeFlag::BarrierRecv as u32
                | PbFragmentTypeFlag::SnapshotBackfillStreamScan as u32))
            != 0
    }

    pub fn dml_rate_limit_fragments(fragment_type_mask: u32) -> bool {
        (fragment_type_mask & PbFragmentTypeFlag::Dml as u32) != 0
    }

    /// Returns mview actor ids.
    pub fn mview_actor_ids(&self) -> Vec<ActorId> {
        Self::filter_actor_ids(self, |fragment_type_mask| {
            (fragment_type_mask & FragmentTypeFlag::Mview as u32) != 0
        })
        .collect()
    }

    /// Returns actor ids that need to be tracked when creating MV.
    pub fn tracking_progress_actor_ids(&self) -> Vec<ActorId> {
        let mut actor_ids = vec![];
        for fragment in self.fragments.values() {
            if fragment.fragment_type_mask & FragmentTypeFlag::CdcFilter as u32 != 0 {
                // Note: CDC table job contains a StreamScan fragment (StreamCdcScan node) and a CdcFilter fragment.
                // We don't track any fragments' progress.
                return vec![];
            }
            if (fragment.fragment_type_mask
                & (FragmentTypeFlag::Values as u32 | FragmentTypeFlag::StreamScan as u32))
                != 0
            {
                actor_ids.extend(fragment.actors.iter().map(|actor| actor.actor_id));
            }
        }
        actor_ids
    }

    /// Returns the fragment with the `Mview` type flag.
    pub fn mview_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Mview as u32) != 0
            })
            .cloned()
    }

    pub fn source_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Source as u32) != 0
            })
            .cloned()
    }

    pub fn sink_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Sink as u32) != 0
            })
            .cloned()
    }

    /// Returns actors that contains backfill executors.
    pub fn backfill_actor_ids(&self) -> HashSet<ActorId> {
        Self::filter_actor_ids(self, |fragment_type_mask| {
            (fragment_type_mask & FragmentTypeFlag::StreamScan as u32) != 0
        })
        .collect()
    }

    pub fn snapshot_backfill_actor_ids(&self) -> HashSet<ActorId> {
        Self::filter_actor_ids(self, |mask| {
            (mask & FragmentTypeFlag::SnapshotBackfillStreamScan as u32) != 0
        })
        .collect()
    }

    /// Extract the fragments that include source executors that contains an external stream source,
    /// grouping by source id.
    pub fn stream_source_fragments(&self) -> HashMap<SourceId, BTreeSet<FragmentId>> {
        let mut source_fragments = HashMap::new();

        for fragment in self.fragments() {
            for actor in &fragment.actors {
                if let Some(source_id) = actor.nodes.as_ref().unwrap().find_stream_source() {
                    source_fragments
                        .entry(source_id)
                        .or_insert(BTreeSet::new())
                        .insert(fragment.fragment_id as FragmentId);

                    break;
                }
            }
        }
        source_fragments
    }

    pub fn source_backfill_fragments(
        &self,
    ) -> MetadataModelResult<HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>> {
        let mut source_fragments = HashMap::new();

        for fragment in self.fragments() {
            for actor in &fragment.actors {
                if let Some(source_id) = actor.nodes.as_ref().unwrap().find_source_backfill() {
                    if fragment.upstream_fragment_ids.len() != 1 {
                        return Err(anyhow::anyhow!("SourceBackfill should have only one upstream fragment, found {:?} for fragment {}", fragment.upstream_fragment_ids, fragment.fragment_id).into());
                    }
                    source_fragments
                        .entry(source_id)
                        .or_insert(BTreeSet::new())
                        .insert((fragment.fragment_id, fragment.upstream_fragment_ids[0]));

                    break;
                }
            }
        }
        Ok(source_fragments)
    }

    /// Resolve dependent table
    fn resolve_dependent_table(stream_node: &StreamNode, table_ids: &mut HashMap<TableId, usize>) {
        let table_id = match stream_node.node_body.as_ref() {
            Some(NodeBody::StreamScan(stream_scan)) => Some(TableId::new(stream_scan.table_id)),
            Some(NodeBody::StreamCdcScan(stream_scan)) => Some(TableId::new(stream_scan.table_id)),
            _ => None,
        };
        if let Some(table_id) = table_id {
            table_ids.entry(table_id).or_default().add_assign(1);
        }

        for child in &stream_node.input {
            Self::resolve_dependent_table(child, table_ids);
        }
    }

    /// Returns a mapping of dependent table ids of the `TableFragments`
    /// to their corresponding count.
    pub fn dependent_table_ids(&self) -> HashMap<TableId, usize> {
        let mut table_ids = HashMap::new();
        self.fragments.values().for_each(|fragment| {
            let actor = &fragment.actors[0];
            Self::resolve_dependent_table(actor.nodes.as_ref().unwrap(), &mut table_ids);
        });

        table_ids
    }

    /// Returns states of actors group by worker id.
    pub fn worker_actor_states(&self) -> BTreeMap<WorkerId, Vec<(ActorId, ActorState)>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.worker_id() as WorkerId;
            map.entry(node_id)
                .or_insert_with(Vec::new)
                .push((actor_id, actor_status.state()));
        }
        map
    }

    /// Returns actor locations group by worker id.
    pub fn worker_actor_ids(&self) -> BTreeMap<WorkerId, Vec<ActorId>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.worker_id() as WorkerId;
            map.entry(node_id).or_insert_with(Vec::new).push(actor_id);
        }
        map
    }

    /// Returns the status of actors group by worker id.
    pub fn worker_actors(&self, include_inactive: bool) -> BTreeMap<WorkerId, Vec<StreamActor>> {
        let mut actors = BTreeMap::default();
        for fragment in self.fragments.values() {
            for actor in &fragment.actors {
                let node_id = self.actor_status[&actor.actor_id].worker_id() as WorkerId;
                if !include_inactive
                    && self.actor_status[&actor.actor_id].state == ActorState::Inactive as i32
                {
                    continue;
                }
                actors
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(actor.clone());
            }
        }
        actors
    }

    /// Returns actor map: `actor_id` => `StreamActor`.
    pub fn actor_map(&self) -> HashMap<ActorId, StreamActor> {
        let mut actor_map = HashMap::default();
        self.fragments.values().for_each(|fragment| {
            fragment.actors.iter().for_each(|actor| {
                actor_map.insert(actor.actor_id, actor.clone());
            });
        });
        actor_map
    }

    pub fn mv_table_id(&self) -> Option<u32> {
        if self
            .fragments
            .values()
            .flat_map(|f| f.state_table_ids.iter())
            .any(|table_id| *table_id == self.table_id.table_id)
        {
            Some(self.table_id.table_id)
        } else {
            None
        }
    }

    /// Returns the internal table ids without the mview table.
    pub fn internal_table_ids(&self) -> Vec<u32> {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.clone())
            .filter(|&t| t != self.table_id.table_id)
            .collect_vec()
    }

    /// Returns all internal table ids including the mview table.
    pub fn all_table_ids(&self) -> impl Iterator<Item = u32> + '_ {
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
