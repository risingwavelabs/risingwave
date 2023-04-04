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

use std::str::FromStr;

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::connector_service::SourceType as PbSourceType;
use risingwave_rpc_client::ConnectorClient;

use crate::source::cdc::{CdcProperties, CdcSplit};
use crate::source::SplitEnumerator;

pub const DATABASE_SERVERS_KEY: &str = "database.servers";

#[derive(Debug)]
pub struct DebeziumSplitEnumerator {
    /// The source_id in the catalog
    source_id: u32,
    source_type: PbSourceType,
    worker_node_addrs: Vec<HostAddr>,
}

#[async_trait]
impl SplitEnumerator for DebeziumSplitEnumerator {
    type Properties = CdcProperties;
    type Split = CdcSplit;

    async fn new(props: CdcProperties) -> anyhow::Result<DebeziumSplitEnumerator> {
        tracing::info!("start validate cdc properties");
        let connector_client = ConnectorClient::new(
            HostAddr::from_str(&props.connector_node_addr)
                .map_err(|e| anyhow!("parse connector node endpoint fail. {}", e))?,
        )
        .await?;

        let server_addrs = props
            .props
            .get(DATABASE_SERVERS_KEY)
            .map(|s| {
                s.split(',')
                    .map(|s| HostAddr::from_str(s))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        let source_type = props.get_source_type()?;
        // validate connector properties
        connector_client
            .validate_source_properties(
                props.source_id as u64,
                props.get_source_type()?,
                props.props,
                props.table_schema,
            )
            .await?;

        tracing::info!("validate properties success");
        Ok(Self {
            source_id: props.source_id,
            source_type,
            worker_node_addrs: server_addrs,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<CdcSplit>> {
        match self.source_type {
            PbSourceType::Mysql | PbSourceType::Postgres => {
                // CDC source only supports single split
                let splits = vec![CdcSplit {
                    split_id: self.source_id,
                    server_addr: None,
                    start_offset: None,
                }];
                Ok(splits)
            }
            PbSourceType::Citus => {
                let splits = self
                    .worker_node_addrs
                    .iter()
                    .enumerate()
                    .map(|(id, addr)| CdcSplit {
                        split_id: id as u32,
                        server_addr: Some(addr.to_string()),
                        start_offset: None,
                    })
                    .collect_vec();
                Ok(splits)
            }
            _ => Err(anyhow!("unexpected source type")),
        }
    }
}
