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

use risingwave_rpc_client::HummockMetaClient;

use crate::common::MetaServiceOpts;

pub async fn list_version() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let version = meta_client.get_current_version().await?;

    let cur_version = meta_client.disable_commit_epoch().await?;
    assert_eq!(version.id, cur_version.id);
    assert_eq!(version.max_committed_epoch, cur_version.max_committed_epoch);

    println!("{:#?}", version);
    Ok(())
}
