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

use risingwave_hummock_sdk::HummockEpoch;
use risingwave_hummock_sdk::version::HummockVersion;

use crate::CtlContext;

pub async fn disable_commit_epoch(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let version = meta_client.disable_commit_epoch().await?;
    println!(
        "Disabled.\
        Current version: id {}",
        version.id,
    );
    Ok(())
}

pub async fn pause_version_checkpoint(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    meta_client
        .risectl_pause_hummock_version_checkpoint()
        .await?;
    println!("Hummock version checkpoint is paused");
    Ok(())
}

pub async fn resume_version_checkpoint(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    meta_client
        .risectl_resume_hummock_version_checkpoint()
        .await?;
    println!("Hummock version checkpoint is resumed");
    Ok(())
}

/// For now this function itself doesn't provide useful info.
/// We can extend it to reveal interested info, e.g. at which hummock version is a user key
/// added/removed for what reason (row deletion/compaction/etc.).
pub async fn replay_version(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let mut base_version = HummockVersion::from_rpc_protobuf(
        &meta_client
            .risectl_get_checkpoint_hummock_version()
            .await?
            .checkpoint_version
            .unwrap(),
    );
    println!("replay starts");
    println!("base version {}", base_version.id);
    let delta_fetch_size = 100;
    let mut current_delta_id = base_version.next_version_id();
    loop {
        let deltas = meta_client
            .list_version_deltas(current_delta_id, delta_fetch_size, HummockEpoch::MAX)
            .await
            .unwrap();
        if deltas.is_empty() {
            break;
        }
        for delta in deltas {
            if delta.prev_id != base_version.id {
                eprintln!("missing delta log for version {}", base_version.id);
                break;
            }
            base_version.apply_version_delta(&delta);
            println!("replayed version {}", base_version.id);
        }
        current_delta_id = base_version.next_version_id();
    }
    println!("replay ends");
    Ok(())
}
