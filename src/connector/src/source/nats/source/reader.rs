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

use anyhow::{anyhow, Result};
use async_nats::jetstream::consumer;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;

use super::NatsOffset;
use crate::parser::ParserConfig;
use crate::source::common::{into_chunk_stream, CommonSplitReader};
use crate::source::nats::NatsProperties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SplitImpl, SplitReader,
};

pub struct NatsSplitReader {
    consumer: consumer::Consumer<consumer::pull::Config>,
    properties: NatsProperties,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    start_position: NatsOffset,
}

#[async_trait]
impl SplitReader for NatsSplitReader {
    type Properties = NatsProperties;

    async fn new(
        properties: NatsProperties,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // TODO: to simplify the logic, return 1 split for first version
        assert!(splits.len() == 1);
        let split = splits.into_iter().next().unwrap().into_nats().unwrap();
        let start_position = match &split.start_sequence {
            NatsOffset::None => match &properties.scan_startup_mode {
                None => NatsOffset::Earliest,
                Some(mode) => match mode.as_str() {
                    "latest" => NatsOffset::Latest,
                    "earliest" => NatsOffset::Earliest,
                    "timestamp_millis" => {
                        if let Some(time) = &properties.start_time {
                            NatsOffset::Timestamp(time.parse()?)
                        } else {
                            return Err(anyhow!("scan_startup_timestamp_millis is required"));
                        }
                    }
                    "sequence_number" => {
                        if let Some(seq) = &properties.start_sequence {
                            NatsOffset::SequenceNumber(seq.clone())
                        } else {
                            return Err(anyhow!("scan_startup_sequence_number is required"));
                        }
                    }
                    _ => {
                        return Err(anyhow!(
                            "invalid scan_startup_mode, accept earliest/latest/sequence_number/time"
                        ))
                    }
                },
            },
            start_position => start_position.to_owned(),
        };

        if !matches!(start_position, NatsOffset::SequenceNumber(_))
            && properties.start_sequence.is_some()
        {
            return Err(
                anyhow!("scan.startup.mode need to be set to 'sequence_number' if you want to start with a specific sequence number")
            );
        }
        if !matches!(start_position, NatsOffset::Timestamp(_)) && properties.start_time.is_some() {
            return Err(
                anyhow!("scan.startup.mode need to be set to 'timestamp_millis' if you want to start with a specific timestamp millis")
            );
        }
        let consumer = properties
            .common
            .build_consumer(0, start_position.clone())
            .await?;
        Ok(Self {
            consumer,
            properties,
            parser_config,
            source_ctx,
            start_position,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self, parser_config, source_context)
    }
}

impl CommonSplitReader for NatsSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        let capacity = self.source_ctx.source_ctrl_opts.chunk_size;
        let messages = self.consumer.messages().await?;
        #[for_await]
        for msgs in messages.ready_chunks(capacity) {
            let mut msg_vec = Vec::with_capacity(capacity);
            for msg in msgs {
                msg_vec.push(SourceMessage::from_nats_jetstream_message(msg?));
            }
            yield msg_vec;
        }
    }
}
