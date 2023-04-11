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

use risingwave_pb::catalog::connection::Info;

use crate::common::CtlContext;

pub async fn list_connections(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let connections = meta_client.list_connections(None).await?;

    for conn in connections {
        println!(
            "Connection#{}, service_name: {}, {}",
            conn.id,
            conn.name,
            match conn.info {
                Some(Info::PrivateLinkService(svc)) => format!(
                    "PrivateLink: endpoint_id: {}, dns_entries: {:?}",
                    svc.endpoint_id, svc.dns_entries,
                ),
                None => "None".to_string(),
            }
        );
    }
    Ok(())
}
