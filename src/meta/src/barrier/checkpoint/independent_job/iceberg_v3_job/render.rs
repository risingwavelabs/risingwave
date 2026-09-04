// Copyright 2026 RisingWave Labs
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

use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::super::RenderedIndependentJobActors;
use crate::MetaResult;
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{DownstreamFragmentRelation, FragmentDownstreamRelation, StreamActor};

/// The resolver fragment is rendered and placed with the whole Iceberg graph, but remains
/// disconnected until a compaction command activates it.
#[derive(Debug)]
pub(super) struct RenderedResolver {
    pub fragment_info: InflightFragmentInfo,
    /// Pre-rendered actor definitions retained for the compaction activation path.
    #[allow(dead_code)]
    pub stream_actors: Vec<StreamActor>,
    /// The disconnected resolver-to-writer edge retained for compaction activation.
    #[allow(dead_code)]
    pub downstream_relation: DownstreamFragmentRelation,
}

/// Split the dormant resolver from an Iceberg graph after actor placement and before edge
/// construction. The complete relation map must be used during rendering so a no-shuffle
/// resolver edge participates in actor alignment.
pub(super) fn partition_resolver(
    mut rendered: RenderedIndependentJobActors,
    downstreams: &mut FragmentDownstreamRelation,
) -> MetaResult<(RenderedIndependentJobActors, RenderedResolver)> {
    let writer_fragment = rendered
        .fragment_infos
        .values()
        .find(|fragment| find_writer_node(&fragment.nodes).is_some())
        .ok_or_else(|| anyhow::anyhow!("Iceberg V3 graph has no pk-index writer"))?;
    let writer_fragment_id = writer_fragment.fragment_id;
    let writer_node = find_writer_node(&writer_fragment.nodes)
        .expect("writer fragment was selected by its writer node");
    let [_, resolver_input] = writer_node.input.as_slice() else {
        return Err(anyhow::anyhow!("Iceberg writer must have exactly two inputs").into());
    };
    let Some(NodeBody::Merge(resolver_merge)) = resolver_input.node_body.as_ref() else {
        return Err(anyhow::anyhow!("Iceberg writer resolver input must be a Merge node").into());
    };
    let resolver_fragment_id = resolver_merge.upstream_fragment_id;

    let resolver_fragment = rendered
        .fragment_infos
        .remove(&resolver_fragment_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Iceberg writer resolver fragment {} was not rendered",
                resolver_fragment_id
            )
        })?;
    if !contains_resolver(&resolver_fragment.nodes) {
        return Err(anyhow::anyhow!(
            "Iceberg writer resolver input fragment {} has no resolver node",
            resolver_fragment_id
        )
        .into());
    }
    let resolver_actors = rendered
        .stream_actors
        .remove(&resolver_fragment_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Iceberg writer resolver fragment {} has no rendered actors",
                resolver_fragment_id
            )
        })?;

    let downstream_relation = {
        let relations = downstreams.get_mut(&resolver_fragment_id).ok_or_else(|| {
            anyhow::anyhow!(
                "resolver fragment {} has no downstream relation",
                resolver_fragment_id
            )
        })?;
        let [relation] = relations.as_slice() else {
            return Err(anyhow::anyhow!(
                "expected resolver fragment {} to have exactly one downstream relation, found {}",
                resolver_fragment_id,
                relations.len(),
            )
            .into());
        };
        if relation.downstream_fragment_id != writer_fragment_id {
            return Err(anyhow::anyhow!(
                "resolver fragment {} points to fragment {} instead of writer fragment {}",
                resolver_fragment_id,
                relation.downstream_fragment_id,
                writer_fragment_id,
            )
            .into());
        }
        relations.remove(0)
    };
    downstreams.remove(&resolver_fragment_id);
    if downstreams
        .values()
        .flatten()
        .any(|relation| relation.downstream_fragment_id == resolver_fragment_id)
    {
        return Err(anyhow::anyhow!(
            "resolver fragment {} must not have an upstream fragment",
            resolver_fragment_id
        )
        .into());
    }

    Ok((
        rendered,
        RenderedResolver {
            fragment_info: resolver_fragment,
            stream_actors: resolver_actors,
            downstream_relation,
        },
    ))
}

fn find_writer_node(node: &PbStreamNode) -> Option<&PbStreamNode> {
    if matches!(node.node_body, Some(NodeBody::IcebergWithPkIndexWriter(_))) {
        return Some(node);
    }
    node.input.iter().find_map(find_writer_node)
}

fn contains_resolver(node: &PbStreamNode) -> bool {
    matches!(node.node_body, Some(NodeBody::CompactionResolver(_)))
        || node.input.iter().any(contains_resolver)
}
