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

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::LazyLock;

use anyhow::Context;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{
    generate_internal_table_name_with_type, TableId, CDC_SOURCE_COLUMN_NUM, TABLE_NAME_COLUMN_NAME,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::stream_graph_visitor;
use risingwave_common::util::stream_graph_visitor::visit_fragment;
use risingwave_pb::catalog::Table;
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_fragment_graph::{
    Parallelism, StreamFragment, StreamFragmentEdge as StreamFragmentEdgeProto,
};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, FragmentTypeFlag, StreamActor,
    StreamFragmentGraph as StreamFragmentGraphProto,
};

use crate::manager::{IdGeneratorManagerRef, StreamingJob};
use crate::model::FragmentId;
use crate::stream::stream_graph::id::{GlobalFragmentId, GlobalFragmentIdGen, GlobalTableIdGen};
use crate::stream::stream_graph::schedule::Distribution;
use crate::MetaResult;

/// The fragment in the building phase, including the [`StreamFragment`] from the frontend and
/// several additional helper fields.
#[derive(Debug, Clone)]
pub(super) struct BuildingFragment {
    /// The fragment structure from the frontend, with the global fragment ID.
    inner: StreamFragment,

    /// The ID of the job if it's materialized in this fragment.
    table_id: Option<u32>,

    /// The required columns of each upstream table.
    upstream_table_columns: HashMap<TableId, Vec<i32>>,
}

impl BuildingFragment {
    /// Create a new [`BuildingFragment`] from a [`StreamFragment`]. The global fragment ID and
    /// global table IDs will be correctly filled with the given `id` and `table_id_gen`.
    fn new(
        id: GlobalFragmentId,
        fragment: StreamFragment,
        job: &StreamingJob,
        table_id_gen: GlobalTableIdGen,
    ) -> Self {
        let mut fragment = StreamFragment {
            fragment_id: id.as_global_id(),
            ..fragment
        };

        // Fill the information of the internal tables in the fragment.
        Self::fill_internal_tables(&mut fragment, job, table_id_gen);

        let table_id = Self::fill_job(&mut fragment, job).then(|| job.id());
        let upstream_table_columns = Self::extract_upstream_table_columns(&mut fragment);

        Self {
            inner: fragment,
            table_id,
            upstream_table_columns,
        }
    }

    /// Extract the internal tables from the fragment.
    fn extract_internal_tables(&self) -> Vec<Table> {
        let mut fragment = self.inner.to_owned();
        let mut tables = Vec::new();
        stream_graph_visitor::visit_internal_tables(&mut fragment, |table, _| {
            tables.push(table.clone());
        });
        tables
    }

    /// Fill the information of the internal tables in the fragment.
    fn fill_internal_tables(
        fragment: &mut StreamFragment,
        job: &StreamingJob,
        table_id_gen: GlobalTableIdGen,
    ) {
        let fragment_id = fragment.fragment_id;
        stream_graph_visitor::visit_internal_tables(fragment, |table, table_type_name| {
            table.id = table_id_gen.to_global_id(table.id).as_global_id();
            table.schema_id = job.schema_id();
            table.database_id = job.database_id();
            table.name = generate_internal_table_name_with_type(
                &job.name(),
                fragment_id,
                table.id,
                table_type_name,
            );
            table.fragment_id = fragment_id;
            table.owner = job.owner();
        });
    }

    /// Fill the information of the job in the fragment.
    fn fill_job(fragment: &mut StreamFragment, job: &StreamingJob) -> bool {
        let table_id = job.id();
        let fragment_id = fragment.fragment_id;
        let mut has_table = false;

        stream_graph_visitor::visit_fragment(fragment, |node_body| match node_body {
            NodeBody::Materialize(materialize_node) => {
                materialize_node.table_id = table_id;

                // Fill the ID of the `Table`.
                let table = materialize_node.table.as_mut().unwrap();
                table.id = table_id;
                table.database_id = job.database_id();
                table.schema_id = job.schema_id();
                table.fragment_id = fragment_id;
                #[cfg(not(debug_assertions))]
                {
                    table.definition = job.name();
                }

                has_table = true;
            }
            NodeBody::Sink(sink_node) => {
                sink_node.sink_desc.as_mut().unwrap().id = table_id;

                has_table = true;
            }
            NodeBody::Dml(dml_node) => {
                dml_node.table_id = table_id;
                dml_node.table_version_id = job.table_version_id().unwrap();
            }
            NodeBody::Source(_) => {
                // Notice: Table job has a dumb Source node, we should be careful that `has_table` should not be overwrite to `false`
                if !has_table {
                    has_table = job.is_source_job();
                }
            }
            _ => {}
        });

        has_table
    }

    /// Extract the required columns (in IDs) of each upstream table.
    fn extract_upstream_table_columns(
        // TODO: no need to take `&mut` here
        fragment: &mut StreamFragment,
    ) -> HashMap<TableId, Vec<i32>> {
        let mut table_columns = HashMap::new();

        stream_graph_visitor::visit_fragment(fragment, |node_body| {
            if let NodeBody::Chain(chain_node) = node_body {
                let table_id = chain_node.table_id.into();
                let column_ids = chain_node.upstream_column_ids.clone();
                table_columns
                    .try_insert(table_id, column_ids)
                    .expect("currently there should be no two same upstream tables in a fragment");
            }
        });

        assert_eq!(table_columns.len(), fragment.upstream_table_ids.len());

        table_columns
    }
}

impl Deref for BuildingFragment {
    type Target = StreamFragment;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// The ID of an edge in the fragment graph. For different types of edges, the ID will be in
/// different variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumAsInner)]
pub(super) enum EdgeId {
    /// The edge between two building (internal) fragments.
    Internal {
        /// The ID generated by the frontend, generally the operator ID of `Exchange`.
        /// See [`StreamFragmentEdgeProto`].
        link_id: u64,
    },

    /// The edge between an upstream external fragment and downstream building fragment. Used for
    /// MV on MV.
    UpstreamExternal {
        /// The ID of the upstream table or materialized view.
        upstream_table_id: TableId,
        /// The ID of the downstream fragment.
        downstream_fragment_id: GlobalFragmentId,
    },

    /// The edge between an upstream building fragment and downstream external fragment. Used for
    /// schema change (replace table plan).
    DownstreamExternal {
        /// The ID of the original upstream fragment (`Materialize`).
        original_upstream_fragment_id: GlobalFragmentId,
        /// The ID of the downstream fragment.
        downstream_fragment_id: GlobalFragmentId,
    },
}

/// The edge in the fragment graph.
///
/// The edge can be either internal or external. This is distinguished by the [`EdgeId`].
#[derive(Debug, Clone)]
pub(super) struct StreamFragmentEdge {
    /// The ID of the edge.
    pub id: EdgeId,

    /// The strategy used for dispatching the data.
    pub dispatch_strategy: DispatchStrategy,
}

impl StreamFragmentEdge {
    fn from_protobuf(edge: &StreamFragmentEdgeProto) -> Self {
        Self {
            // By creating an edge from the protobuf, we know that the edge is from the frontend and
            // is internal.
            id: EdgeId::Internal {
                link_id: edge.link_id,
            },
            dispatch_strategy: edge.get_dispatch_strategy().unwrap().clone(),
        }
    }
}

/// In-memory representation of a **Fragment** Graph, built from the [`StreamFragmentGraphProto`]
/// from the frontend.
#[derive(Default)]
pub struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<GlobalFragmentId, BuildingFragment>,

    /// stores edges between fragments: upstream => downstream.
    downstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// stores edges between fragments: downstream -> upstream.
    upstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// Dependent relations of this job.
    dependent_table_ids: HashSet<TableId>,

    /// The default parallelism of the job, specified by the `STREAMING_PARALLELISM` session
    /// variable. If not specified, all active parallel units will be used.
    default_parallelism: Option<NonZeroUsize>,
}

impl StreamFragmentGraph {
    /// Create a new [`StreamFragmentGraph`] from the given [`StreamFragmentGraphProto`], with all
    /// global IDs correctly filled.
    pub async fn new(
        proto: StreamFragmentGraphProto,
        id_gen: IdGeneratorManagerRef,
        job: &StreamingJob,
    ) -> MetaResult<Self> {
        let fragment_id_gen =
            GlobalFragmentIdGen::new(&id_gen, proto.fragments.len() as u64).await?;
        let table_id_gen = GlobalTableIdGen::new(&id_gen, proto.table_ids_cnt as u64).await?;

        // Create nodes.
        let fragments: HashMap<_, _> = proto
            .fragments
            .into_iter()
            .map(|(id, fragment)| {
                let id = fragment_id_gen.to_global_id(id);
                let fragment = BuildingFragment::new(id, fragment, job, table_id_gen);
                (id, fragment)
            })
            .collect();

        assert_eq!(
            fragments
                .values()
                .map(|f| f.extract_internal_tables().len() as u32)
                .sum::<u32>(),
            proto.table_ids_cnt
        );

        // Create edges.
        let mut downstreams = HashMap::new();
        let mut upstreams = HashMap::new();

        for edge in proto.edges {
            let upstream_id = fragment_id_gen.to_global_id(edge.upstream_id);
            let downstream_id = fragment_id_gen.to_global_id(edge.downstream_id);
            let edge = StreamFragmentEdge::from_protobuf(&edge);

            upstreams
                .entry(downstream_id)
                .or_insert_with(HashMap::new)
                .try_insert(upstream_id, edge.clone())
                .unwrap();
            downstreams
                .entry(upstream_id)
                .or_insert_with(HashMap::new)
                .try_insert(downstream_id, edge)
                .unwrap();
        }

        // Note: Here we directly use the field `dependent_table_ids` in the proto (resolved in
        // frontend), instead of visiting the graph ourselves.
        let dependent_table_ids = proto
            .dependent_table_ids
            .iter()
            .map(TableId::from)
            .collect();

        let default_parallelism = if let Some(Parallelism { parallelism }) = proto.parallelism {
            Some(NonZeroUsize::new(parallelism as usize).context("parallelism should not be 0")?)
        } else {
            None
        };

        Ok(Self {
            fragments,
            downstreams,
            upstreams,
            dependent_table_ids,
            default_parallelism,
        })
    }

    /// Retrieve the internal tables map of the whole graph.
    pub fn internal_tables(&self) -> HashMap<u32, Table> {
        let mut tables = HashMap::new();
        for fragment in self.fragments.values() {
            for table in fragment.extract_internal_tables() {
                let table_id = table.id;
                tables
                    .try_insert(table_id, table)
                    .unwrap_or_else(|_| panic!("duplicated table id `{}`", table_id));
            }
        }
        tables
    }

    /// Set internal tables' `table_id`s according to a list of internal tables
    pub fn fit_internal_table_ids(
        &mut self,
        mut old_internal_tables: Vec<Table>,
    ) -> MetaResult<()> {
        let mut new_internal_table_ids = Vec::new();
        for fragment in self.fragments.values() {
            for table in &fragment.extract_internal_tables() {
                new_internal_table_ids.push(table.id);
            }
        }

        if new_internal_table_ids.len() != old_internal_tables.len() {
            bail!(
                "Different number of internal tables. New: {}, Old: {}",
                new_internal_table_ids.len(),
                old_internal_tables.len()
            );
        }
        old_internal_tables.sort_by(|a, b| a.id.cmp(&b.id));
        new_internal_table_ids.sort();

        let internal_table_id_map = new_internal_table_ids
            .into_iter()
            .zip_eq_fast(old_internal_tables.into_iter())
            .collect::<HashMap<_, _>>();

        for fragment in self.fragments.values_mut() {
            stream_graph_visitor::visit_internal_tables(
                &mut fragment.inner,
                |table, _table_type_name| {
                    let target = internal_table_id_map.get(&table.id).cloned().unwrap();
                    *table = target;
                },
            );
        }

        Ok(())
    }

    /// Returns the fragment id where the table is materialized.
    pub fn table_fragment_id(&self) -> FragmentId {
        self.fragments
            .values()
            .filter(|b| b.table_id.is_some())
            .map(|b| b.fragment_id)
            .exactly_one()
            .expect("require exactly 1 materialize/sink node when creating the streaming job")
    }

    /// Returns the fragment id where the table dml is received.
    pub fn dml_fragment_id(&self) -> Option<FragmentId> {
        self.fragments
            .values()
            .filter(|b| b.fragment_type_mask & FragmentTypeFlag::Dml as u32 != 0)
            .map(|b| b.fragment_id)
            .at_most_one()
            .expect("require at most 1 dml node when creating the streaming job")
    }

    /// Get the dependent streaming job ids of this job.
    pub fn dependent_table_ids(&self) -> &HashSet<TableId> {
        &self.dependent_table_ids
    }

    /// Get the default parallelism of the job.
    pub fn default_parallelism(&self) -> Option<NonZeroUsize> {
        self.default_parallelism
    }

    /// Get downstreams of a fragment.
    fn get_downstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        self.downstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }

    /// Get upstreams of a fragment.
    fn get_upstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        self.upstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }
}

static EMPTY_HASHMAP: LazyLock<HashMap<GlobalFragmentId, StreamFragmentEdge>> =
    LazyLock::new(HashMap::new);

/// A fragment that is either being built or already exists. Used for generalize the logic of
/// [`crate::stream::ActorGraphBuilder`].
#[derive(Debug, Clone, EnumAsInner)]
pub(super) enum EitherFragment {
    /// An internal fragment that is being built for the current streaming job.
    Building(BuildingFragment),

    /// An existing fragment that is external but connected to the fragments being built.
    Existing(Fragment),
}

/// A wrapper of [`StreamFragmentGraph`] that contains the additional information of existing
/// fragments, which is connected to the graph's top-most or bottom-most fragments.
///
/// For example,
/// - if we're going to build a mview on an existing mview, the upstream fragment containing the
///   `Materialize` node will be included in this structure.
/// - if we're going to replace the plan of a table with downstream mviews, the downstream fragments
///   containing the `Chain` nodes will be included in this structure.
pub struct CompleteStreamFragmentGraph {
    /// The fragment graph of the streaming job being built.
    building_graph: StreamFragmentGraph,

    /// The required information of existing fragments.
    existing_fragments: HashMap<GlobalFragmentId, Fragment>,

    /// Extra edges between existing fragments and the building fragments.
    extra_downstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// Extra edges between existing fragments and the building fragments.
    extra_upstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,
}

pub struct FragmentGraphUpstreamContext {
    /// Root fragment is the root of upstream stream graph, which can be a
    /// mview fragment or source fragment for cdc source job
    upstream_root_fragments: HashMap<TableId, Fragment>,
}

pub struct FragmentGraphDownstreamContext {
    original_table_fragment_id: FragmentId,
    downstream_fragments: Vec<(DispatchStrategy, Fragment)>,
}

impl CompleteStreamFragmentGraph {
    /// Create a new [`CompleteStreamFragmentGraph`] with empty existing fragments, i.e., there's no
    /// upstream mviews.
    #[cfg(test)]
    pub fn for_test(graph: StreamFragmentGraph) -> Self {
        Self {
            building_graph: graph,
            existing_fragments: Default::default(),
            extra_downstreams: Default::default(),
            extra_upstreams: Default::default(),
        }
    }

    /// Create a new [`CompleteStreamFragmentGraph`] for MV on MV or Table on CDC Source, with the upstream existing
    /// `Materialize` or `Source` fragments.
    pub fn with_upstreams(
        graph: StreamFragmentGraph,
        upstream_root_fragments: HashMap<TableId, Fragment>,
        table_job_type: Option<TableJobType>,
    ) -> MetaResult<Self> {
        Self::build_helper(
            graph,
            Some(FragmentGraphUpstreamContext {
                upstream_root_fragments,
            }),
            None,
            table_job_type,
        )
    }

    /// Create a new [`CompleteStreamFragmentGraph`] for replacing an existing table, with the
    /// downstream existing `Chain` fragments.
    pub fn with_downstreams(
        graph: StreamFragmentGraph,
        original_table_fragment_id: FragmentId,
        downstream_fragments: Vec<(DispatchStrategy, Fragment)>,
    ) -> MetaResult<Self> {
        Self::build_helper(
            graph,
            None,
            Some(FragmentGraphDownstreamContext {
                original_table_fragment_id,
                downstream_fragments,
            }),
            None,
        )
    }

    fn build_helper(
        mut graph: StreamFragmentGraph,
        upstream_ctx: Option<FragmentGraphUpstreamContext>,
        downstream_ctx: Option<FragmentGraphDownstreamContext>,
        table_job_type: Option<TableJobType>,
    ) -> MetaResult<Self> {
        let mut extra_downstreams = HashMap::new();
        let mut extra_upstreams = HashMap::new();
        let mut existing_fragments = HashMap::new();

        if let Some(FragmentGraphUpstreamContext {
            upstream_root_fragments,
        }) = upstream_ctx
        {
            // Build the extra edges between the upstream `Materialize` and the downstream `Chain`
            // of the new materialized view.
            for (&id, fragment) in &mut graph.fragments {
                for (&upstream_table_id, output_columns) in &fragment.upstream_table_columns {
                    let (up_fragment_id, edge) = match table_job_type.as_ref() {
                        Some(TableJobType::SharedCdcSource) => {
                            // extract the upstream full_table_name from the source fragment
                            let mut full_table_name = None;
                            visit_fragment(&mut fragment.inner, |node_body| {
                                if let NodeBody::Chain(chain_node) = node_body {
                                    full_table_name = chain_node
                                        .cdc_table_desc
                                        .as_ref()
                                        .map(|desc| desc.table_name.clone());
                                }
                            });

                            let source_fragment =
                                upstream_root_fragments
                                    .get(&upstream_table_id)
                                    .context("upstream materialized view fragment not found")?;
                            let source_job_id = GlobalFragmentId::new(source_fragment.fragment_id);
                            // extract `_rw_table_name` column index
                            let rw_table_name_index = {
                                let node = source_fragment.actors[0].get_nodes().unwrap();

                                // may remove the expect to extend other scenarios, currently only target the CDC scenario
                                node.fields
                                    .iter()
                                    .position(|f| f.name.as_str() == TABLE_NAME_COLUMN_NAME)
                                    .expect("table name column not found")
                            };

                            assert!(full_table_name.is_some());
                            tracing::debug!(
                                ?full_table_name,
                                ?source_job_id,
                                ?rw_table_name_index,
                                ?output_columns,
                                "chain with upstream source fragment"
                            );
                            let edge = StreamFragmentEdge {
                                id: EdgeId::UpstreamExternal {
                                    upstream_table_id,
                                    downstream_fragment_id: id,
                                },
                                dispatch_strategy: DispatchStrategy {
                                    r#type: DispatcherType::CdcTablename as _, /* there may have multiple downstream table jobs, so we use `Hash` here */
                                    dist_key_indices: vec![rw_table_name_index as _], /* index to `_rw_table_name` column */
                                    output_indices: (0..CDC_SOURCE_COLUMN_NUM as _).collect_vec(), /* require all columns from the cdc source */
                                    downstream_table_name: full_table_name,
                                },
                            };

                            (source_job_id, edge)
                        }
                        _ => {
                            // handle other kinds of streaming graph, normally MV on MV
                            let mview_fragment = upstream_root_fragments
                                .get(&upstream_table_id)
                                .context("upstream materialized view fragment not found")?;
                            let mview_id = GlobalFragmentId::new(mview_fragment.fragment_id);

                            // Resolve the required output columns from the upstream materialized view.
                            let output_indices = {
                                let nodes = mview_fragment.actors[0].get_nodes().unwrap();
                                let mview_node =
                                    nodes.get_node_body().unwrap().as_materialize().unwrap();
                                let all_column_ids = mview_node
                                    .get_table()
                                    .unwrap()
                                    .columns
                                    .iter()
                                    .map(|c| c.column_desc.as_ref().unwrap().column_id)
                                    .collect_vec();

                                output_columns
                                    .iter()
                                    .map(|c| {
                                        all_column_ids
                                            .iter()
                                            .position(|&id| id == *c)
                                            .map(|i| i as u32)
                                    })
                                    .collect::<Option<Vec<_>>>()
                                    .context("column not found in the upstream materialized view")?
                            };
                            let edge = StreamFragmentEdge {
                                id: EdgeId::UpstreamExternal {
                                    upstream_table_id,
                                    downstream_fragment_id: id,
                                },
                                // We always use `NoShuffle` for the exchange between the upstream
                                // `Materialize` and the downstream `Chain` of the
                                // new materialized view.
                                dispatch_strategy: DispatchStrategy {
                                    r#type: DispatcherType::NoShuffle as _,
                                    dist_key_indices: vec![], // not used for `NoShuffle`
                                    output_indices,
                                    downstream_table_name: None,
                                },
                            };

                            (mview_id, edge)
                        }
                    };

                    // put the edge into the extra edges
                    extra_downstreams
                        .entry(up_fragment_id)
                        .or_insert_with(HashMap::new)
                        .try_insert(id, edge.clone())
                        .unwrap();
                    extra_upstreams
                        .entry(id)
                        .or_insert_with(HashMap::new)
                        .try_insert(up_fragment_id, edge)
                        .unwrap();
                }
            }

            existing_fragments.extend(
                upstream_root_fragments
                    .into_values()
                    .map(|f| (GlobalFragmentId::new(f.fragment_id), f)),
            );
        }

        if let Some(FragmentGraphDownstreamContext {
            original_table_fragment_id,
            downstream_fragments,
        }) = downstream_ctx
        {
            let original_table_fragment_id = GlobalFragmentId::new(original_table_fragment_id);
            let table_fragment_id = GlobalFragmentId::new(graph.table_fragment_id());

            // Build the extra edges between the `Materialize` and the downstream `Chain` of the
            // existing materialized views.
            for (dispatch_strategy, fragment) in &downstream_fragments {
                let id = GlobalFragmentId::new(fragment.fragment_id);

                let edge = StreamFragmentEdge {
                    id: EdgeId::DownstreamExternal {
                        original_upstream_fragment_id: original_table_fragment_id,
                        downstream_fragment_id: id,
                    },
                    dispatch_strategy: dispatch_strategy.clone(),
                };

                extra_downstreams
                    .entry(table_fragment_id)
                    .or_insert_with(HashMap::new)
                    .try_insert(id, edge.clone())
                    .unwrap();
                extra_upstreams
                    .entry(id)
                    .or_insert_with(HashMap::new)
                    .try_insert(table_fragment_id, edge)
                    .unwrap();
            }

            existing_fragments.extend(
                downstream_fragments
                    .into_iter()
                    .map(|(_, f)| (GlobalFragmentId::new(f.fragment_id), f)),
            );
        }

        Ok(Self {
            building_graph: graph,
            existing_fragments,
            extra_downstreams,
            extra_upstreams,
        })
    }
}

impl CompleteStreamFragmentGraph {
    /// Returns **all** fragment IDs in the complete graph, including the ones that are not in the
    /// building graph.
    pub(super) fn all_fragment_ids(&self) -> impl Iterator<Item = GlobalFragmentId> + '_ {
        self.building_graph
            .fragments
            .keys()
            .chain(self.existing_fragments.keys())
            .copied()
    }

    /// Returns an iterator of **all** edges in the complete graph, including the external edges.
    pub(super) fn all_edges(
        &self,
    ) -> impl Iterator<Item = (GlobalFragmentId, GlobalFragmentId, &StreamFragmentEdge)> + '_ {
        self.building_graph
            .downstreams
            .iter()
            .chain(self.extra_downstreams.iter())
            .flat_map(|(&from, tos)| tos.iter().map(move |(&to, edge)| (from, to, edge)))
    }

    /// Returns the distribution of the existing fragments.
    pub(super) fn existing_distribution(&self) -> HashMap<GlobalFragmentId, Distribution> {
        self.existing_fragments
            .iter()
            .map(|(&id, f)| (id, Distribution::from_fragment(f)))
            .collect()
    }

    /// Generate topological order of **all** fragments in this graph, including the ones that are
    /// not in the building graph. Returns error if the graph is not a DAG and topological sort can
    /// not be done.
    ///
    /// For MV on MV, the first fragment popped out from the heap will be the top-most node, or the
    /// `Sink` / `Materialize` in stream graph.
    pub(super) fn topo_order(&self) -> MetaResult<Vec<GlobalFragmentId>> {
        let mut topo = Vec::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all fragments.
        for fragment_id in self.all_fragment_ids() {
            // Count how many downstreams we have for a given fragment.
            let downstream_cnt = self.get_downstreams(fragment_id).count();
            if downstream_cnt == 0 {
                topo.push(fragment_id);
            } else {
                downstream_cnts.insert(fragment_id, downstream_cnt);
            }
        }

        let mut i = 0;
        while let Some(&fragment_id) = topo.get(i) {
            i += 1;
            // Find if we can process more fragments.
            for (upstream_id, _) in self.get_upstreams(fragment_id) {
                let downstream_cnt = downstream_cnts.get_mut(&upstream_id).unwrap();
                *downstream_cnt -= 1;
                if *downstream_cnt == 0 {
                    downstream_cnts.remove(&upstream_id);
                    topo.push(upstream_id);
                }
            }
        }

        if !downstream_cnts.is_empty() {
            // There are fragments that are not processed yet.
            bail!("graph is not a DAG");
        }

        Ok(topo)
    }

    /// Seal a [`BuildingFragment`] from the graph into a [`Fragment`], which will be further used
    /// to build actors on the compute nodes and persist into meta store.
    pub(super) fn seal_fragment(
        &self,
        id: GlobalFragmentId,
        actors: Vec<StreamActor>,
        distribution: Distribution,
    ) -> Fragment {
        let building_fragment = self.get_fragment(id).into_building().unwrap();
        let internal_tables = building_fragment.extract_internal_tables();
        let BuildingFragment {
            inner,
            table_id,
            upstream_table_columns: _,
        } = building_fragment;

        let distribution_type = distribution.to_distribution_type() as i32;

        let state_table_ids = internal_tables
            .iter()
            .map(|t| t.id)
            .chain(table_id)
            .collect();

        let upstream_fragment_ids = self
            .get_upstreams(id)
            .map(|(id, _)| id.as_global_id())
            .collect();

        Fragment {
            fragment_id: inner.fragment_id,
            fragment_type_mask: inner.fragment_type_mask,
            distribution_type,
            actors,
            vnode_mapping: Some(distribution.into_mapping().to_protobuf()),
            state_table_ids,
            upstream_fragment_ids,
        }
    }

    /// Get a fragment from the complete graph, which can be either a building fragment or an
    /// existing fragment.
    pub(super) fn get_fragment(&self, fragment_id: GlobalFragmentId) -> EitherFragment {
        if let Some(fragment) = self.existing_fragments.get(&fragment_id) {
            EitherFragment::Existing(fragment.clone())
        } else {
            EitherFragment::Building(
                self.building_graph
                    .fragments
                    .get(&fragment_id)
                    .unwrap()
                    .clone(),
            )
        }
    }

    /// Get **all** downstreams of a fragment, including the ones that are not in the building
    /// graph.
    pub(super) fn get_downstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> impl Iterator<Item = (GlobalFragmentId, &StreamFragmentEdge)> {
        self.building_graph
            .get_downstreams(fragment_id)
            .iter()
            .chain(
                self.extra_downstreams
                    .get(&fragment_id)
                    .into_iter()
                    .flatten(),
            )
            .map(|(&id, edge)| (id, edge))
    }

    /// Get **all** upstreams of a fragment, including the ones that are not in the building
    /// graph.
    pub(super) fn get_upstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> impl Iterator<Item = (GlobalFragmentId, &StreamFragmentEdge)> {
        self.building_graph
            .get_upstreams(fragment_id)
            .iter()
            .chain(self.extra_upstreams.get(&fragment_id).into_iter().flatten())
            .map(|(&id, edge)| (id, edge))
    }

    /// Returns all building fragments in the graph.
    pub(super) fn building_fragments(&self) -> &HashMap<GlobalFragmentId, BuildingFragment> {
        &self.building_graph.fragments
    }
}
