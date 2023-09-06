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
use futures_async_stream::try_stream;
use jni::objects::{JObject, JValue};
use risingwave_common::util::addr::HostAddr;
use risingwave_jni_core::jvm_runtime::JVM;
use risingwave_jni_core::GetEventStreamJniSender;
use risingwave_pb::connector_service::GetEventStreamResponse;
use tokio::sync::mpsc;

use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::cdc::CdcProperties;
use crate::source::common::{into_chunk_stream, CommonSplitReader};
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SplitId, SplitImpl, SplitMetaData,
    SplitReader,
};

pub struct CdcSplitReader {
    source_id: u64,
    start_offset: Option<String>,
    // host address of worker node for a Citus cluster
    server_addr: Option<String>,
    conn_props: CdcProperties,

    split_id: SplitId,
    // whether the full snapshot phase is done
    snapshot_done: bool,
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
        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();
        let split_id = split.id();
        match split {
            SplitImpl::MySqlCdc(split) | SplitImpl::PostgresCdc(split) => Ok(Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: None,
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
            }),
            SplitImpl::CitusCdc(split) => Ok(Self {
                source_id: split.split_id() as u64,
                start_offset: split.start_offset().clone(),
                server_addr: split.server_addr().clone(),
                conn_props,
                split_id,
                snapshot_done: split.snapshot_done(),
                parser_config,
                source_ctx,
            }),

            _ => Err(anyhow!(
                "failed to create cdc split reader: invalid splis info"
            )),
        }
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self, parser_config, source_context)
    }
}

impl CommonSplitReader for CdcSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
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

        let (tx, mut rx) = mpsc::channel(1024);

        let tx: Box<GetEventStreamJniSender> = Box::new(tx);

        let source_type = self.conn_props.get_source_type_pb()?;

        JVM.as_ref()
            .ok_or_else(|| anyhow!("JVM is not initialized, so fail to create cdc table"))?;

        std::thread::spawn(move || {
            let mut env = JVM
                .as_ref()
                .unwrap()
                .attach_current_thread_as_daemon()
                .unwrap();

            let st = env
                .call_static_method(
                    "com/risingwave/proto/ConnectorServiceProto$SourceType",
                    "forNumber",
                    "(I)Lcom/risingwave/proto/ConnectorServiceProto$SourceType;",
                    &[JValue::from(source_type as i32)],
                )
                .inspect_err(|e| tracing::error!("jni call error: {:?}", e))
                .unwrap();

            let st = env.call_static_method(
                "com/risingwave/connector/api/source/SourceTypeE",
                "valueOf",
                "(Lcom/risingwave/proto/ConnectorServiceProto$SourceType;)Lcom/risingwave/connector/api/source/SourceTypeE;",
                &[(&st).into()]
                )
                .inspect_err(|e| tracing::error!("jni call error: {:?}", e))
                .unwrap();

            let start_offset = match self.start_offset {
                Some(start_offset) => {
                    let start_offset = env.new_string(start_offset).unwrap();
                    env.call_method(start_offset, "toString", "()Ljava/lang/String;", &[])
                        .unwrap()
                }
                None => jni::objects::JValueGen::Object(JObject::null()),
            };

            let java_map = env.new_object("java/util/HashMap", "()V", &[]).unwrap();

            for (key, value) in &properties {
                let key = env.new_string(key.to_string()).unwrap();
                let value = env.new_string(value.to_string()).unwrap();
                let args = [JValue::Object(&key), JValue::Object(&value)];
                env.call_method(
                    &java_map,
                    "put",
                    "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                    &args,
                )
                .inspect_err(|e| tracing::error!("jni call error: {:?}", e))
                .unwrap();
            }

            let channel_ptr = Box::into_raw(tx) as i64;
            let channel_ptr = JValue::from(channel_ptr);

            let _ = env.call_static_method(
                "com/risingwave/connector/source/core/JniDbzSourceHandler",
                "runJniDbzSourceThread",
                "(Lcom/risingwave/connector/api/source/SourceTypeE;JLjava/lang/String;Ljava/util/Map;ZJ)V",
                &[(&st).into(), JValue::from(self.source_id as i64), (&start_offset).into(), JValue::Object(&java_map), JValue::from(self.snapshot_done), channel_ptr]
                )
                .inspect_err(|e| tracing::error!("jni call error: {:?}", e))
                .unwrap();
        });

        while let Some(GetEventStreamResponse { events, .. }) = rx.recv().await {
            tracing::debug!("receive events {:?}", events.len());
            if events.is_empty() {
                continue;
            }
            let mut msgs = Vec::with_capacity(events.len());
            for event in events {
                msgs.push(SourceMessage::from(event));
            }
            yield msgs;
        }
    }
}
