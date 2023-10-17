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
use std::sync::LazyLock;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures_async_stream::try_stream;
use itertools::Itertools;
use jni::objects::JValue;
use prost::Message;
use risingwave_common::util::addr::HostAddr;
use risingwave_jni_core::jvm_runtime::JVM;
use risingwave_jni_core::GetEventStreamJniSender;
use risingwave_pb::connector_service::{GetEventStreamRequest, GetEventStreamResponse};
use tokio::sync::mpsc;

use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::cdc::{CdcProperties, CdcSourceType, CdcSourceTypeTrait, DebeziumCdcSplit};
use crate::source::{
    into_chunk_stream, BoxSourceWithStateStream, Column, CommonSplitReader, SourceContextRef,
    SplitId, SplitMetaData, SplitReader,
};

pub struct CdcSplitReader<T: CdcSourceTypeTrait> {
    source_id: u64,
    start_offset: Option<String>,
    // host address of worker node for a Citus cluster
    server_addr: Option<String>,
    conn_props: CdcProperties<T>,

    split_id: SplitId,
    // whether the full snapshot phase is done
    snapshot_done: bool,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

const DEFAULT_CHANNEL_SIZE: usize = 16;

#[async_trait]
impl<T: CdcSourceTypeTrait> SplitReader for CdcSplitReader<T> {
    type Properties = CdcProperties<T>;
    type Split = DebeziumCdcSplit<T>;

    #[allow(clippy::unused_async)]
    async fn new(
        conn_props: CdcProperties<T>,
        splits: Vec<DebeziumCdcSplit<T>>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();
        let split_id = split.id();
        match T::source_type() {
            CdcSourceType::Mysql | CdcSourceType::Postgres => Ok(Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: None,
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
            }),
            CdcSourceType::Citus => Ok(Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: split.server_addr().clone(),
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
            }),
        }
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self, parser_config, source_context)
    }
}

impl<T: CdcSourceTypeTrait> CommonSplitReader for CdcSplitReader<T> {
    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        // rewrite the hostname and port for the split
        let mut properties = self.conn_props.props.clone();

        // For citus, we need to rewrite the table.name to capture sharding tables
        if self.server_addr.is_some() {
            let addr = self.server_addr.unwrap();
            let host_addr = HostAddr::from_str(&addr)
                .map_err(|err| anyhow!("invalid server address for cdc split. {}", err))?;
            properties.insert("hostname".to_string(), host_addr.host);
            properties.insert("port".to_string(), host_addr.port.to_string());
            // rewrite table name with suffix to capture all shards in the split
            let mut table_name = properties
                .remove("table.name")
                .ok_or_else(|| anyhow!("missing field 'table.name'"))?;
            table_name.push_str("_[0-9]+");
            properties.insert("table.name".into(), table_name);
        }

        let (tx, mut rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        LazyLock::force(&JVM).as_ref()?;

        let get_event_stream_request = GetEventStreamRequest {
            source_id: self.source_id,
            source_type: self.conn_props.get_source_type_pb() as _,
            start_offset: self.start_offset.unwrap_or_default(),
            properties,
            snapshot_done: self.snapshot_done,
        };

        let source_id = get_event_stream_request.source_id.to_string();
        let source_type = get_event_stream_request.source_type.to_string();

        std::thread::spawn(move || {
            let mut env = JVM.as_ref().unwrap().attach_current_thread().unwrap();

            let get_event_stream_request_bytes = env
                .byte_array_from_slice(&Message::encode_to_vec(&get_event_stream_request))
                .unwrap();
            let result = env.call_static_method(
                "com/risingwave/connector/source/core/JniDbzSourceHandler",
                "runJniDbzSourceThread",
                "([BJ)V",
                &[
                    JValue::Object(&get_event_stream_request_bytes),
                    JValue::from(&tx as *const GetEventStreamJniSender as i64),
                ],
            );

            match result {
                Ok(_) => {
                    tracing::info!("end of jni call runJniDbzSourceThread");
                }
                Err(e) => {
                    tracing::error!("jni call error: {:?}", e);
                }
            }
        });

        while let Some(GetEventStreamResponse { events, .. }) = rx.recv().await {
            tracing::trace!("receive events {:?}", events.len());
            self.source_ctx
                .metrics
                .connector_source_rows_received
                .with_label_values(&[&source_type, &source_id])
                .inc_by(events.len() as u64);
            let msgs = events.into_iter().map(SourceMessage::from).collect_vec();
            yield msgs;
        }

        Err(anyhow!("all senders are dropped"))?;
    }
}
