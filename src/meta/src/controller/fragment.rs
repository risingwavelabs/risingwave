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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem::swap;

use anyhow::Context;
use risingwave_common::bail;
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_meta_model_v2::{
    actor, fragment, ActorStatus, ConnectorSplits, Dispatchers, FragmentVnodeMapping, StreamNode,
    TableId, VnodeBitmap,
};
use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
use risingwave_pb::meta::table_fragments::{PbActorStatus, PbFragment, PbState};
use risingwave_pb::meta::PbTableFragments;
use risingwave_pb::source::PbConnectorSplits;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbStreamEnvironment, StreamActor};

use crate::controller::catalog::CatalogController;
use crate::MetaResult;

impl CatalogController {
    pub fn extract_fragment_and_actors_from_table_fragments(
        PbTableFragments {
            table_id,
            fragments,
            actor_status,
            actor_splits,
            ..
        }: PbTableFragments,
    ) -> MetaResult<Vec<(fragment::Model, Vec<actor::Model>)>> {
        let mut result = vec![];

        let fragments: BTreeMap<_, _> = fragments.into_iter().collect();

        for (_, fragment) in fragments {
            let (fragment, actors) = Self::extract_fragment_and_actors(
                table_id as _,
                fragment,
                &actor_status,
                &actor_splits,
            )?;

            result.push((fragment, actors));
        }

        Ok(result)
    }

    pub fn extract_fragment_and_actors(
        table_id: TableId,
        pb_fragment: PbFragment,
        pb_actor_status: &HashMap<u32, PbActorStatus>,
        pb_actor_splits: &HashMap<u32, PbConnectorSplits>,
    ) -> MetaResult<(fragment::Model, Vec<actor::Model>)> {
        let PbFragment {
            fragment_id: pb_fragment_id,
            fragment_type_mask: pb_fragment_type_mask,
            distribution_type: pb_distribution_type,
            actors: pb_actors,
            vnode_mapping: pb_vnode_mapping,
            state_table_ids: pb_state_table_ids,
            upstream_fragment_ids: pb_upstream_fragment_ids,
        } = pb_fragment;

        let state_table_ids = pb_state_table_ids.into();

        assert!(!pb_actors.is_empty());

        let stream_node = {
            let actor_template = pb_actors.first().cloned().unwrap();
            let mut stream_node = actor_template.nodes.unwrap();
            visit_stream_node(&mut stream_node, |body| {
                if let NodeBody::Merge(m) = body {
                    m.upstream_actor_id = vec![];
                }
            });

            stream_node
        };

        let mut actors = vec![];

        for mut actor in pb_actors {
            let mut upstream_actors = BTreeMap::new();

            let node = actor.nodes.as_mut().context("nodes is empty")?;

            visit_stream_node(node, |body| {
                if let NodeBody::Merge(m) = body {
                    let mut upstream_actor_ids = vec![];
                    swap(&mut m.upstream_actor_id, &mut upstream_actor_ids);
                    assert!(
                        upstream_actors
                            .insert(m.upstream_fragment_id, upstream_actor_ids)
                            .is_none(),
                        "There should only be one link between two fragments"
                    );
                }
            });

            let StreamActor {
                actor_id,
                fragment_id,
                nodes: _,
                dispatcher: pb_dispatcher,
                upstream_actor_id: pb_upstream_actor_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition: _,
            } = actor;

            let splits = pb_actor_splits.get(&actor_id).cloned().map(ConnectorSplits);
            let status = pb_actor_status.get(&actor_id).cloned().map(ActorStatus);

            let status = status.ok_or_else(|| {
                anyhow::anyhow!(
                    "actor {} in fragment {} has no actor_status",
                    actor_id,
                    fragment_id
                )
            })?;

            let parallel_unit_id = status
                .inner_ref()
                .parallel_unit
                .as_ref()
                .map(|parallel_unit| parallel_unit.id)
                .expect("no parallel unit id found in actor_status")
                as _;

            assert_eq!(
                pb_upstream_actor_id
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                upstream_actors
                    .values()
                    .flatten()
                    .cloned()
                    .collect::<BTreeSet<_>>()
            );

            actors.push(actor::Model {
                actor_id: actor_id as _,
                fragment_id: fragment_id as _,
                status,
                splits,
                parallel_unit_id,
                upstream_actor_ids: upstream_actors.into(),
                dispatchers: Dispatchers(pb_dispatcher),
                vnode_bitmap: pb_vnode_bitmap.map(VnodeBitmap),
            });
        }

        let upstream_fragment_id = pb_upstream_fragment_ids.into();

        let vnode_mapping = pb_vnode_mapping.map(FragmentVnodeMapping);

        let stream_node = StreamNode(stream_node);

        let distribution_type = PbFragmentDistributionType::try_from(pb_distribution_type)
            .unwrap()
            .into();

        let fragment = fragment::Model {
            fragment_id: pb_fragment_id as _,
            table_id,
            fragment_type_mask: pb_fragment_type_mask as _,
            distribution_type,
            stream_node,
            vnode_mapping,
            state_table_ids,
            upstream_fragment_id,
        };

        Ok((fragment, actors))
    }

    pub fn compose_table_fragments(
        table_id: u32,
        state: PbState,
        env: Option<PbStreamEnvironment>,
        fragments: Vec<(fragment::Model, Vec<actor::Model>)>,
    ) -> MetaResult<PbTableFragments> {
        let mut pb_fragments = HashMap::new();
        let mut pb_actor_splits = HashMap::new();
        let mut pb_actor_status = HashMap::new();

        for (fragment, actors) in fragments {
            let (fragment, fragment_actor_status, fragment_actor_splits) =
                Self::compose_fragment(fragment, actors)?;

            pb_fragments.insert(fragment.fragment_id, fragment);

            pb_actor_splits.extend(fragment_actor_splits.into_iter());
            pb_actor_status.extend(fragment_actor_status.into_iter());
        }

        let table_fragments = PbTableFragments {
            table_id,
            state: state as _,
            fragments: pb_fragments,
            actor_status: pb_actor_status,
            actor_splits: pb_actor_splits,
            env,
        };

        Ok(table_fragments)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn compose_fragment(
        fragment: fragment::Model,
        actors: Vec<actor::Model>,
    ) -> MetaResult<(
        PbFragment,
        HashMap<u32, PbActorStatus>,
        HashMap<u32, PbConnectorSplits>,
    )> {
        let fragment::Model {
            fragment_id,
            table_id: _,
            fragment_type_mask,
            distribution_type,
            stream_node,
            vnode_mapping,
            state_table_ids,
            upstream_fragment_id,
        } = fragment;

        let stream_node_template = stream_node.into_inner();

        let mut pb_actors = vec![];

        let mut pb_actor_status = HashMap::new();
        let mut pb_actor_splits = HashMap::new();

        for actor in actors {
            if actor.fragment_id != fragment_id {
                bail!(
                    "fragment id {} from actor {} is different from fragment {}",
                    actor.fragment_id,
                    actor.actor_id,
                    fragment_id
                )
            }

            let actor::Model {
                actor_id,
                fragment_id,
                status,
                splits,
                upstream_actor_ids,
                dispatchers,
                vnode_bitmap,
                ..
            } = actor;

            let upstream_fragment_actors = upstream_actor_ids.into_inner();

            let pb_nodes = {
                let mut nodes = stream_node_template.clone();

                visit_stream_node(&mut nodes, |body| {
                    if let NodeBody::Merge(m) = body
                        && let Some(upstream_actor_ids) = upstream_fragment_actors.get(&(m.upstream_fragment_id as _))
                    {
                        m.upstream_actor_id = upstream_actor_ids.iter().map(|id| *id as _).collect();
                    }
                });

                Some(nodes)
            };

            let pb_vnode_bitmap = vnode_bitmap.map(|vnode_bitmap| vnode_bitmap.into_inner());

            let pb_upstream_actor_id = upstream_fragment_actors
                .values()
                .flatten()
                .map(|&id| id as _)
                .collect();

            let pb_dispatcher = dispatchers.into_inner();

            pb_actor_status.insert(actor_id as _, status.into_inner());

            if let Some(splits) = splits {
                pb_actor_splits.insert(actor_id as _, splits.into_inner());
            }

            pb_actors.push(StreamActor {
                actor_id: actor_id as _,
                fragment_id: fragment_id as _,
                nodes: pb_nodes,
                dispatcher: pb_dispatcher,
                upstream_actor_id: pb_upstream_actor_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition: "".to_string(),
            })
        }

        let pb_upstream_fragment_ids = upstream_fragment_id.into_u32_array();
        let pb_vnode_mapping = vnode_mapping.map(|mapping| mapping.into_inner());
        let pb_state_table_ids = state_table_ids.into_u32_array();
        let pb_distribution_type = PbFragmentDistributionType::from(distribution_type) as _;
        let pb_fragment = PbFragment {
            fragment_id: fragment_id as _,
            fragment_type_mask: fragment_type_mask as _,
            distribution_type: pb_distribution_type,
            actors: pb_actors,
            vnode_mapping: pb_vnode_mapping,
            state_table_ids: pb_state_table_ids,
            upstream_fragment_ids: pb_upstream_fragment_ids,
        };

        Ok((pb_fragment, pb_actor_status, pb_actor_splits))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::default::Default;

    use itertools::Itertools;
    use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_common::util::stream_graph_visitor::visit_stream_node;
    use risingwave_meta_model_v2::fragment::DistributionType;
    use risingwave_meta_model_v2::{
        actor, fragment, ActorId, ActorStatus, ActorUpstreamActors, ConnectorSplits, Dispatchers,
        FragmentId, FragmentVnodeMapping, I32Array, StreamNode, TableId, VnodeBitmap,
    };
    use risingwave_pb::common::ParallelUnit;
    use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
    use risingwave_pb::meta::table_fragments::{PbActorStatus, PbFragment};
    use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
    use risingwave_pb::stream_plan::stream_node::{NodeBody, PbNodeBody};
    use risingwave_pb::stream_plan::{
        Dispatcher, MergeNode, PbDispatcher, PbDispatcherType, PbFragmentTypeFlag, PbStreamActor,
        PbStreamNode, PbUnionNode, StreamActor,
    };

    use crate::controller::catalog::CatalogController;
    use crate::MetaResult;

    const TEST_FRAGMENT_ID: FragmentId = 1;

    const TEST_UPSTREAM_FRAGMENT_ID: FragmentId = 2;

    const TEST_TABLE_ID: TableId = 1;

    const TEST_STATE_TABLE_ID: TableId = 1000;

    fn generate_parallel_units(count: u32) -> Vec<ParallelUnit> {
        (0..count)
            .map(|parallel_unit_id| ParallelUnit {
                id: parallel_unit_id,
                ..Default::default()
            })
            .collect_vec()
    }

    fn generate_dispatchers_for_actor(actor_id: u32) -> Vec<Dispatcher> {
        vec![PbDispatcher {
            r#type: PbDispatcherType::Hash as _,
            dispatcher_id: actor_id as u64,
            downstream_actor_id: vec![actor_id],
            ..Default::default()
        }]
    }

    fn generate_upstream_actor_ids_for_actor(actor_id: u32) -> BTreeMap<FragmentId, Vec<ActorId>> {
        let mut upstream_actor_ids = BTreeMap::new();
        upstream_actor_ids.insert(TEST_UPSTREAM_FRAGMENT_ID, vec![(actor_id + 100) as ActorId]);
        upstream_actor_ids.insert(
            TEST_UPSTREAM_FRAGMENT_ID + 1,
            vec![(actor_id + 200) as ActorId],
        );
        upstream_actor_ids
    }

    fn generate_merger_stream_node(
        actor_upstream_actor_ids: &BTreeMap<FragmentId, Vec<ActorId>>,
    ) -> PbStreamNode {
        let mut input = vec![];
        for (upstream_fragment_id, upstream_actor_ids) in actor_upstream_actor_ids {
            input.push(PbStreamNode {
                node_body: Some(PbNodeBody::Merge(MergeNode {
                    upstream_actor_id: upstream_actor_ids.iter().map(|id| *id as _).collect(),
                    upstream_fragment_id: *upstream_fragment_id as _,
                    ..Default::default()
                })),
                ..Default::default()
            });
        }

        PbStreamNode {
            input,
            node_body: Some(PbNodeBody::Union(PbUnionNode {})),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_extract_fragment() -> MetaResult<()> {
        let actor_count = 3u32;
        let parallel_units = generate_parallel_units(actor_count);
        let parallel_unit_mapping = ParallelUnitMapping::build(&parallel_units);
        let actor_vnode_bitmaps = parallel_unit_mapping.to_bitmaps();

        let upstream_actor_ids: HashMap<ActorId, BTreeMap<FragmentId, Vec<ActorId>>> = (0
            ..actor_count)
            .map(|actor_id| {
                (
                    actor_id as _,
                    generate_upstream_actor_ids_for_actor(actor_id),
                )
            })
            .collect();

        let pb_actors = (0..actor_count)
            .map(|actor_id| {
                let actor_upstream_actor_ids =
                    upstream_actor_ids.get(&(actor_id as _)).cloned().unwrap();
                let stream_node = generate_merger_stream_node(&actor_upstream_actor_ids);

                PbStreamActor {
                    actor_id: actor_id as _,
                    fragment_id: TEST_FRAGMENT_ID as _,
                    nodes: Some(stream_node),
                    dispatcher: generate_dispatchers_for_actor(actor_id),
                    upstream_actor_id: actor_upstream_actor_ids
                        .values()
                        .flatten()
                        .map(|id| *id as _)
                        .collect(),
                    vnode_bitmap: actor_vnode_bitmaps
                        .get(&actor_id)
                        .cloned()
                        .map(|bitmap| bitmap.to_protobuf()),
                    mview_definition: "".to_string(),
                }
            })
            .collect_vec();

        let pb_fragment = PbFragment {
            fragment_id: TEST_FRAGMENT_ID as _,
            fragment_type_mask: PbFragmentTypeFlag::Source as _,
            distribution_type: PbFragmentDistributionType::Hash as _,
            actors: pb_actors.clone(),
            vnode_mapping: Some(parallel_unit_mapping.to_protobuf()),
            state_table_ids: vec![TEST_STATE_TABLE_ID as _],
            upstream_fragment_ids: upstream_actor_ids
                .values()
                .flat_map(|m| m.keys().map(|x| *x as _))
                .collect(),
        };

        let pb_actor_status = (0..actor_count)
            .map(|actor_id| {
                (
                    actor_id,
                    PbActorStatus {
                        parallel_unit: Some(parallel_units[actor_id as usize].clone()),
                        ..Default::default()
                    },
                )
            })
            .collect();

        let pb_actor_splits = Default::default();

        let (fragment, actors) = CatalogController::extract_fragment_and_actors(
            TEST_TABLE_ID,
            pb_fragment.clone(),
            &pb_actor_status,
            &pb_actor_splits,
        )?;

        check_fragment_template(fragment.clone(), pb_actors.clone(), &upstream_actor_ids);
        check_fragment(fragment, pb_fragment);
        check_actors(actors, pb_actors, pb_actor_status, pb_actor_splits);

        Ok(())
    }

    fn check_fragment_template(
        fragment: fragment::Model,
        actors: Vec<PbStreamActor>,
        upstream_actor_ids: &HashMap<ActorId, BTreeMap<FragmentId, Vec<ActorId>>>,
    ) {
        let stream_node_template = fragment.stream_node.clone();

        for PbStreamActor {
            nodes, actor_id, ..
        } in actors
        {
            let mut template_node = stream_node_template.clone().into_inner();
            let nodes = nodes.unwrap();
            let actor_upstream_actor_ids =
                upstream_actor_ids.get(&(actor_id as _)).cloned().unwrap();
            visit_stream_node(&mut template_node, |body| {
                if let NodeBody::Merge(m) = body {
                    m.upstream_actor_id = actor_upstream_actor_ids
                        .get(&(m.upstream_fragment_id as _))
                        .map(|actors| actors.iter().map(|id| *id as _).collect())
                        .unwrap();
                }
            });

            assert_eq!(nodes, template_node);
        }
    }

    #[tokio::test]
    async fn test_compose_fragment() -> MetaResult<()> {
        let actor_count = 3u32;
        let parallel_units = generate_parallel_units(actor_count);
        let parallel_unit_mapping = ParallelUnitMapping::build(&parallel_units);
        let mut actor_vnode_bitmaps = parallel_unit_mapping.to_bitmaps();

        let upstream_actor_ids: HashMap<ActorId, BTreeMap<FragmentId, Vec<ActorId>>> = (0
            ..actor_count)
            .map(|actor_id| {
                (
                    actor_id as _,
                    generate_upstream_actor_ids_for_actor(actor_id),
                )
            })
            .collect();

        let actors = (0..actor_count)
            .map(|actor_id| {
                let parallel_unit_id = actor_id as ParallelUnitId;

                let vnode_bitmap = actor_vnode_bitmaps
                    .remove(&parallel_unit_id)
                    .map(|m| VnodeBitmap(m.to_protobuf()));

                let actor_status = ActorStatus(PbActorStatus {
                    parallel_unit: Some(parallel_units[actor_id as usize].clone()),
                    ..Default::default()
                });

                let actor_splits = Some(ConnectorSplits(PbConnectorSplits {
                    splits: vec![PbConnectorSplit {
                        split_type: "dummy".to_string(),
                        ..Default::default()
                    }],
                }));

                let actor_upstream_actor_ids =
                    upstream_actor_ids.get(&(actor_id as _)).cloned().unwrap();
                let dispatchers = generate_dispatchers_for_actor(actor_id);

                actor::Model {
                    actor_id: actor_id as ActorId,
                    fragment_id: TEST_FRAGMENT_ID,
                    status: actor_status,
                    splits: actor_splits,
                    parallel_unit_id: parallel_unit_id as i32,
                    upstream_actor_ids: ActorUpstreamActors(actor_upstream_actor_ids),
                    dispatchers: Dispatchers(dispatchers),
                    vnode_bitmap,
                }
            })
            .collect_vec();

        let stream_node = {
            let template_actor = actors.first().cloned().unwrap();

            let template_upstream_actor_ids = template_actor
                .upstream_actor_ids
                .into_inner()
                .into_keys()
                .map(|k| (k, vec![]))
                .collect();

            generate_merger_stream_node(&template_upstream_actor_ids)
        };

        let fragment = fragment::Model {
            fragment_id: TEST_FRAGMENT_ID,
            table_id: TEST_TABLE_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode(stream_node),
            vnode_mapping: Some(FragmentVnodeMapping(parallel_unit_mapping.to_protobuf())),
            state_table_ids: I32Array(vec![TEST_STATE_TABLE_ID]),
            upstream_fragment_id: I32Array::default(),
        };

        let (pb_fragment, pb_actor_status, pb_actor_splits) =
            CatalogController::compose_fragment(fragment.clone(), actors.clone()).unwrap();

        assert_eq!(pb_actor_status.len(), actor_count as usize);
        assert_eq!(pb_actor_splits.len(), actor_count as usize);

        for (actor_id, actor_status) in &pb_actor_status {
            let parallel_unit_id = parallel_units[*actor_id as usize].id;
            assert_eq!(
                parallel_unit_id,
                actor_status.parallel_unit.clone().unwrap().id
            );
        }

        let pb_actors = pb_fragment.actors.clone();

        check_fragment_template(fragment.clone(), pb_actors.clone(), &upstream_actor_ids);
        check_fragment(fragment, pb_fragment);
        check_actors(actors, pb_actors, pb_actor_status, pb_actor_splits);

        Ok(())
    }

    fn check_actors(
        actors: Vec<actor::Model>,
        pb_actors: Vec<StreamActor>,
        pb_actor_status: HashMap<u32, PbActorStatus>,
        pb_actor_splits: HashMap<u32, PbConnectorSplits>,
    ) {
        for (
            actor::Model {
                actor_id,
                fragment_id,
                status,
                splits,
                parallel_unit_id,
                upstream_actor_ids,
                dispatchers,
                vnode_bitmap,
            },
            StreamActor {
                actor_id: pb_actor_id,
                fragment_id: pb_fragment_id,
                nodes: pb_nodes,
                dispatcher: pb_dispatcher,
                upstream_actor_id: pb_upstream_actor_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition,
            },
        ) in actors.into_iter().zip_eq_debug(pb_actors.into_iter())
        {
            assert_eq!(actor_id, pb_actor_id as ActorId);
            assert_eq!(fragment_id, pb_fragment_id as FragmentId);
            assert_eq!(parallel_unit_id, pb_actor_id as i32);
            let upstream_actor_ids = upstream_actor_ids.into_inner();

            assert_eq!(
                upstream_actor_ids
                    .values()
                    .flatten()
                    .map(|id| *id as u32)
                    .collect_vec(),
                pb_upstream_actor_id
            );
            assert_eq!(dispatchers, Dispatchers(pb_dispatcher));
            assert_eq!(
                vnode_bitmap,
                pb_vnode_bitmap
                    .as_ref()
                    .map(|bitmap| VnodeBitmap(bitmap.clone()))
            );

            assert_eq!(mview_definition, "");

            let mut pb_nodes = pb_nodes.unwrap();

            visit_stream_node(&mut pb_nodes, |body| {
                if let PbNodeBody::Merge(m) = body {
                    let upstream_actor_ids = upstream_actor_ids
                        .get(&(m.upstream_fragment_id as _))
                        .map(|actors| actors.iter().map(|id| *id as u32).collect_vec())
                        .unwrap();
                    assert_eq!(upstream_actor_ids, m.upstream_actor_id);
                }
            });

            assert_eq!(
                status,
                pb_actor_status
                    .get(&pb_actor_id)
                    .cloned()
                    .map(ActorStatus)
                    .unwrap()
            );

            assert_eq!(
                splits,
                pb_actor_splits
                    .get(&pb_actor_id)
                    .cloned()
                    .map(ConnectorSplits)
            );
        }
    }

    fn check_fragment(fragment: fragment::Model, pb_fragment: PbFragment) {
        let PbFragment {
            fragment_id,
            fragment_type_mask,
            distribution_type: pb_distribution_type,
            actors: _,
            vnode_mapping: pb_vnode_mapping,
            state_table_ids: pb_state_table_ids,
            upstream_fragment_ids: pb_upstream_fragment_ids,
        } = pb_fragment;

        assert_eq!(fragment_id, TEST_FRAGMENT_ID as u32);
        assert_eq!(fragment_type_mask, fragment.fragment_type_mask as u32);
        assert_eq!(
            pb_distribution_type,
            PbFragmentDistributionType::from(fragment.distribution_type) as i32
        );
        assert_eq!(
            pb_vnode_mapping.map(FragmentVnodeMapping),
            fragment.vnode_mapping
        );

        assert_eq!(
            pb_upstream_fragment_ids,
            fragment.upstream_fragment_id.into_u32_array()
        );

        assert_eq!(
            pb_state_table_ids,
            fragment.state_table_ids.into_u32_array()
        );
    }
}
