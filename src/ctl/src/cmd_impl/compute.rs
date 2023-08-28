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
use std::convert::TryFrom;

use risingwave_common::config::{BatchConfig, StreamingConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_rpc_client::ComputeClient;
use serde_json;

pub async fn show_config(host: &str) -> anyhow::Result<()> {
    let listen_addr = HostAddr::try_from(host)?;
    let client = ComputeClient::new(listen_addr).await?;
    let config_response = client.show_config().await?;
    let batch_config: BatchConfig = serde_json::from_str(&config_response.batch_config)?;
    let stream_config: StreamingConfig = serde_json::from_str(&config_response.stream_config)?;
    println!("{}", serde_json::to_string_pretty(&batch_config)?);
    println!("{}", serde_json::to_string_pretty(&stream_config)?);
    Ok(())
}
