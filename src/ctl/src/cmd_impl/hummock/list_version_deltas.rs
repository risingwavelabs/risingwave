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

use risingwave_hummock_sdk::{HummockEpoch, HummockVersionId};

use crate::CtlContext;

pub async fn list_version_deltas(
    context: &CtlContext,
    start_id: HummockVersionId,
    num_epochs: u32,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let resp = meta_client
        .list_version_deltas(start_id, num_epochs, HummockEpoch::MAX)
        .await?;
    println!("{:#?}", resp);
    Ok(())
}
