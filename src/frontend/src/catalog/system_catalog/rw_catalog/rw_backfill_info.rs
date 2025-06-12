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

use risingwave_common::types::Fields;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_backfills;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::meta::FragmentDistribution;
use risingwave_pb::stream_plan::FragmentTypeFlag;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwBackfillInfo {
    job_id: i32,
    #[primary_key]
    fragment_id: i32,
    backfill_state_table_id: i32,
    backfill_target_table_id: i32,
    is_snapshot_backfill: bool,
    backfill_epoch: i64,
}

fn extract_stream_scan(fragment_distribution: &FragmentDistribution) -> Option<RwBackfillInfo> {
    if fragment_distribution.fragment_type_mask & (FragmentTypeFlag::StreamScan as u32) == 0 {
        return None;
    }

    let stream_node = fragment_distribution.node.as_ref()?;
    let is_snapshot_backfill = fragment_distribution.fragment_type_mask
        & (FragmentTypeFlag::SnapshotBackfillStreamScan as u32
            | FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan as u32)
        != 0;

    let mut scan = None;
    visit_stream_node_backfills(stream_node, |node| {
        scan = Some(RwBackfillInfo {
            job_id: fragment_distribution.table_id as i32,
            fragment_id: fragment_distribution.fragment_id as i32,
            backfill_state_table_id: node
                .state_table
                .as_ref()
                .map(|table| table.id as i32)
                .unwrap_or(0),
            backfill_target_table_id: node.table_id as i32,
            is_snapshot_backfill,
            backfill_epoch: node.snapshot_backfill_epoch() as _,
        });
    });
    scan
}

#[system_catalog(table, "rw_catalog.rw_backfill_info")]
async fn read_rw_backfill_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwBackfillInfo>> {
    let distributions = reader
        .meta_client
        .list_creating_fragment_distribution()
        .await?;

    Ok(distributions
        .into_iter()
        .filter_map(|distribution| extract_stream_scan(&distribution))
        .collect())
}
