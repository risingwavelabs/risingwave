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

use risingwave_pb::hummock::{PinnedSnapshotsSummary, PinnedVersionsSummary};
use risingwave_rpc_client::HummockMetaClient;

use crate::CtlContext;

pub async fn list_version(context: &CtlContext, verbose: bool) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let version = meta_client.get_current_version().await?;

    if verbose {
        println!("{:#?}", version);
    } else {
        println!(
            "Version {} max_committed_epoch {}",
            version.id, version.max_committed_epoch
        );

        for (cg, levels) in &version.levels {
            println!("CompactionGroup {}", cg);

            // l0
            if levels.l0.is_some() {
                for sub_level in levels.l0.as_ref().unwrap().sub_levels.iter().rev() {
                    println!(
                        "sub_level_id {} type {} sst_num {} size {}",
                        sub_level.sub_level_id,
                        sub_level.level_type().as_str_name(),
                        sub_level.table_infos.len(),
                        sub_level.total_file_size
                    )
                }
            }

            for level in levels.get_levels().iter() {
                println!(
                    "level_idx {} type {} sst_num {} size {}",
                    level.level_idx,
                    level.level_type().as_str_name(),
                    level.table_infos.len(),
                    level.total_file_size
                )
            }
        }
    }

    Ok(())
}

pub async fn list_pinned_versions(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let PinnedVersionsSummary {
        mut pinned_versions,
        workers,
    } = meta_client
        .risectl_get_pinned_versions_summary()
        .await?
        .summary
        .unwrap();
    pinned_versions.sort_by_key(|v| v.min_pinned_id);
    for pinned_version in pinned_versions {
        match workers.get(&pinned_version.context_id) {
            None => {
                println!(
                    "Worker {} may have been dropped, min_pinned_version_id {}",
                    pinned_version.context_id, pinned_version.min_pinned_id
                );
            }
            Some(worker) => {
                println!(
                    "Worker {} type {} min_pinned_version_id {}",
                    pinned_version.context_id,
                    worker.r#type().as_str_name(),
                    pinned_version.min_pinned_id
                );
            }
        }
    }
    Ok(())
}

pub async fn list_pinned_snapshots(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let PinnedSnapshotsSummary {
        mut pinned_snapshots,
        workers,
    } = meta_client
        .risectl_get_pinned_snapshots_summary()
        .await?
        .summary
        .unwrap();
    pinned_snapshots.sort_by_key(|s| s.minimal_pinned_snapshot);
    for pinned_snapshot in pinned_snapshots {
        match workers.get(&pinned_snapshot.context_id) {
            None => {
                println!(
                    "Worker {} may have been dropped, min_pinned_snapshot {}",
                    pinned_snapshot.context_id, pinned_snapshot.minimal_pinned_snapshot
                );
            }
            Some(worker) => {
                println!(
                    "Worker {} type {} min_pinned_snapshot {}",
                    pinned_snapshot.context_id,
                    worker.r#type().as_str_name(),
                    pinned_snapshot.minimal_pinned_snapshot
                );
            }
        }
    }
    Ok(())
}
