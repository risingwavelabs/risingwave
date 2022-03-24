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

use risingwave_storage::StateStore;

use crate::common::HummockServiceOpts;

pub async fn list_kv() -> anyhow::Result<()> {
    let hummock_opts = HummockServiceOpts::from_env()?;
    let hummock = hummock_opts.create_hummock_store().await?;
    // TODO: support speficy epoch
    tracing::info!("using u64::MAX as epoch");

    for (k, v) in hummock.scan::<_, Vec<u8>>(.., None, u64::MAX).await? {
        println!("{:?} => {:?}", k, v);
    }

    Ok(())
}
