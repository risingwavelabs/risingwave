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

use risingwave_common::id::{FragmentId, JobId, LocalOperatorId, TableId};
use risingwave_common::types::Fields;
use risingwave_common::util::stream_graph_visitor::visit_backfill_nodes;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::id::RelationId;
use risingwave_pb::meta::FragmentDistribution;
use risingwave_pb::stream_plan::StreamScanType;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::catalog::system_catalog::rw_catalog::common::CatalogBackfillType;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(fragment_id, operator_id)]
struct RwBackfillInfo {
    job_id: JobId,
    fragment_id: FragmentId,
    operator_id: LocalOperatorId,
    backfill_state_table_id: TableId,
    backfill_target_relation_id: RelationId,
    backfill_type: String,
    backfill_epoch: i64,
}

fn extract_backfill_nodes(fragment_distribution: &FragmentDistribution) -> Vec<RwBackfillInfo> {
    let mut backfill_nodes = vec![];
    let Some(stream_node) = fragment_distribution.node.as_ref() else {
        return backfill_nodes;
    };
    visit_backfill_nodes(
        fragment_distribution.fragment_id,
        stream_node,
        |_, _, stream_node| {
            let (state_table, backfill_target_relation_id, backfill_type, backfill_epoch) =
                match stream_node.node_body.as_ref().unwrap() {
                    NodeBody::StreamScan(node) => {
                        let backfill_type = if matches!(
                            node.stream_scan_type(),
                            StreamScanType::SnapshotBackfill
                                | StreamScanType::CrossDbSnapshotBackfill
                        ) {
                            CatalogBackfillType::SnapshotBackfill
                        } else {
                            CatalogBackfillType::ArrangementOrNoShuffle
                        };
                        (
                            &node.state_table,
                            node.table_id.as_relation_id(),
                            backfill_type,
                            node.snapshot_backfill_epoch() as _,
                        )
                    }
                    NodeBody::SourceBackfill(node) => (
                        &node.state_table,
                        node.upstream_source_id.as_relation_id(),
                        CatalogBackfillType::Source,
                        0,
                    ),
                    // A locality provider backfills from its own state table.
                    NodeBody::LocalityProvider(node) => (
                        &node.progress_table,
                        node.state_table
                            .as_ref()
                            .map_or(TableId::placeholder(), |table| table.id)
                            .as_relation_id(),
                        CatalogBackfillType::ArrangementOrNoShuffle,
                        0,
                    ),
                    // `Values` has no state to show.
                    _ => return,
                };
            backfill_nodes.push(RwBackfillInfo {
                job_id: fragment_distribution.table_id,
                fragment_id: fragment_distribution.fragment_id,
                operator_id: stream_node.operator_id.into(),
                backfill_state_table_id: state_table
                    .as_ref()
                    .map_or(TableId::placeholder(), |table| table.id),
                backfill_target_relation_id,
                backfill_type: backfill_type.to_string(),
                backfill_epoch,
            });
        },
    );
    backfill_nodes
}

#[system_catalog(table, "rw_catalog.rw_backfill_info")]
async fn read_rw_backfill_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwBackfillInfo>> {
    let distributions = reader
        .meta_client
        .list_creating_fragment_distribution()
        .await?;

    Ok(distributions
        .iter()
        .flat_map(extract_backfill_nodes)
        .collect())
}
