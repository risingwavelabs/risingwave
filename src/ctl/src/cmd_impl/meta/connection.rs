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

use risingwave_frontend::WithOptions;
use risingwave_pb::catalog::connection::Info;
use risingwave_pb::cloud_service::SourceType;
use risingwave_sqlparser::parser::Parser;

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
                None => "None".to_string(),
            }
        );
    }
    Ok(())
}

pub async fn validate_source(context: &CtlContext, sql: String) -> anyhow::Result<()> {
    let statements = Parser::parse_sql(sql.as_ref()).expect("error parsing sql");
    let with_options = WithOptions::try_from(statements.get(0).expect("no sql statement found"))
        .expect("error parsing with clause");
    let source_type = match with_options
        .get("connector")
        .expect("missing 'connector' in with clause")
        .as_str()
    {
        "kafka" => SourceType::Kafka,
        _ => SourceType::Unspecified,
    };
    let meta_client = context.meta_client().await?;
    let resp = meta_client
        .rw_cloud_validate_source(
            source_type,
            with_options
                .inner()
                .iter()
                .map(|(key, val)| (key.to_owned(), val.to_owned()))
                .collect(),
        )
        .await?;
    println!("validate result: {:#?}", resp);
    Ok(())
}
