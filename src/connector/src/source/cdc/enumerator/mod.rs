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

use async_trait::async_trait;
use risingwave_common::util::addr::HostAddr;
use risingwave_rpc_client::ConnectorClient;

use crate::source::cdc::{CdcProperties, CdcSplit};
use crate::source::SplitEnumerator;

#[derive(Debug)]
pub struct DebeziumSplitEnumerator {
    /// The source_id in the catalog
    source_id: u32,
}

#[async_trait]
impl SplitEnumerator for DebeziumSplitEnumerator {
    type Properties = CdcProperties;
    type Split = CdcSplit;

    async fn new(props: CdcProperties) -> anyhow::Result<DebeziumSplitEnumerator> {
        let cdc_client =
            ConnectorClient::new(HostAddr::from_str(&props.connector_node_addr)?).await?;

        // validate connector properties
        cdc_client
            .validate_source_properties(
                props.source_id as u64,
                props.source_type_enum()?,
                props.props,
                props.table_schema,
            )
            .await?;

        tracing::debug!("validate properties success");
        Ok(Self {
            source_id: props.source_id,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<CdcSplit>> {
        // CDC source only supports single split
        let splits = vec![CdcSplit {
            source_id: self.source_id,
            start_offset: None,
        }];
        Ok(splits)
    }
}
