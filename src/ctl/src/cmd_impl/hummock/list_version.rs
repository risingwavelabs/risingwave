// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::hummock::{PinnedSnapshotsSummary, PinnedVersionsSummary};
use risingwave_rpc_client::HummockMetaClient;

use crate::common::MetaServiceOpts;

pub async fn list_version() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let version = meta_client.get_current_version().await?;
    println!("{:#?}", version);
    Ok(())
}

pub async fn list_pinned_versions() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
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
                    pinned_version.context_id, worker.r#type, pinned_version.min_pinned_id
                );
            }
        }
    }
    Ok(())
}

pub async fn list_pinned_snapshots() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
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
                    worker.r#type,
                    pinned_snapshot.minimal_pinned_snapshot
                );
            }
        }
    }
    Ok(())
}
