// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;

use anyhow::anyhow;
use risingwave_pb::catalog::connection::Info;
use risingwave_pb::cloud_service::SourceType;
use serde_json::Value;

use crate::common::CtlContext;

pub async fn list_connections(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let connections = meta_client.list_connections(None).await?;

    for conn in connections {
        println!(
            "Connection#{}, connection_name: {}, {}",
            conn.id,
            conn.name,
            match conn.info {
                Some(Info::PrivateLinkService(svc)) => format!(
                    "PrivateLink: service_name: {}, endpoint_id: {}, dns_entries: {:?}",
                    svc.service_name, svc.endpoint_id, svc.dns_entries,
                ),
                Some(Info::ConnectionParams(params)) => {
                    format!(
                        "CONNECTION_PARAMS_{}: {}",
                        params.get_connection_type().unwrap().as_str_name(),
                        serde_json::to_string(&params.get_properties()).unwrap()
                    )
                }
                None => "None".to_string(),
            }
        );
    }
    Ok(())
}

pub async fn validate_source(context: &CtlContext, props: String) -> anyhow::Result<()> {
    let with_props: HashMap<String, String> =
        serde_json::from_str::<HashMap<String, Value>>(props.as_str())
            .expect("error parsing with props json")
            .into_iter()
            .map(|(key, val)| match val {
                Value::String(s) => (key, s),
                _ => (key, val.to_string()),
            })
            .collect();
    let source_type = match with_props
        .get("connector")
        .expect("missing 'connector' in with clause")
        .as_str()
    {
        "kafka" => Ok(SourceType::Kafka),
        _ => Err(anyhow!(
            "unsupported source type, only kafka sources are supported"
        )),
    }?;
    let meta_client = context.meta_client().await?;
    let resp = meta_client
        .rw_cloud_validate_source(source_type, with_props)
        .await?;
    if !resp.ok {
        eprintln!("{}", serde_json::to_string(&resp).unwrap());
        std::process::exit(1);
    }
    Ok(())
}
