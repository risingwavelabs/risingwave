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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{pin_mut, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::connector_service::GetEventStreamResponse;
use risingwave_rpc_client::ConnectorClient;

use crate::impl_common_split_reader_logic;
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::cdc::CdcProperties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SplitId, SplitImpl, SplitMetaData,
    SplitReader,
};

impl_common_split_reader_logic!(CdcSplitReader, CdcProperties);

pub struct CdcSplitReader {
    source_id: u64,
    start_offset: Option<String>,
    conn_props: CdcProperties,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for CdcSplitReader {
    type Properties = CdcProperties;

    #[allow(clippy::unused_async)]
    async fn new(
        conn_props: CdcProperties,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        assert!(splits.len() == 1);
        let split = splits.into_iter().next().unwrap();
        let split_id = split.id();
        match split {
            SplitImpl::MySqlCdc(split) | SplitImpl::PostgresCdc(split) => Ok(Self {
                source_id: split.source_id as u64,
                start_offset: split.start_offset,
                conn_props,
                split_id,
                parser_config,
                source_ctx,
            }),
            _ => Err(anyhow!(
                "failed to create cdc split reader: invalid splis info"
            )),
        }
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}

impl CdcSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        tracing::debug!("cdc props: {:?}", self.conn_props);
        let cdc_client =
            ConnectorClient::new(HostAddr::from_str(&self.conn_props.connector_node_addr)?).await?;

        let cdc_stream = cdc_client
            .start_source_stream(
                self.source_id,
                self.conn_props.source_type_enum()?,
                self.start_offset,
                self.conn_props.props,
            )
            .await
            .inspect_err(|err| tracing::error!("connector node start stream error: {}", err))?;
        pin_mut!(cdc_stream);
        #[for_await]
        for event_res in cdc_stream {
            match event_res {
                Ok(GetEventStreamResponse { events, .. }) => {
                    if events.is_empty() {
                        continue;
                    }
                    let mut msgs = Vec::with_capacity(events.len());
                    for event in events {
                        msgs.push(SourceMessage::from(event));
                    }
                    yield msgs;
                }
                Err(e) => {
                    return Err(anyhow!(
                        "Cdc service error: code {}, msg {}",
                        e.code(),
                        e.message()
                    ))
                }
            }
        }
    }
}
