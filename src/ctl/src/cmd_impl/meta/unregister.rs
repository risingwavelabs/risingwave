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

use risingwave_common::util::addr::HostAddr;

use crate::CtlContext;

// TODO: rename into cordon
/// mark node as unschedulable
pub async fn unregister_worker_node(context: &CtlContext, addr: HostAddr) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    meta_client.unregister(addr).await?;
    // TODO: automatically call clear_worker_node here?
    Ok(())
}
