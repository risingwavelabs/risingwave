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

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;

use crate::parser::ParserConfig;
use crate::source::common::{into_chunk_stream, CommonSplitReader};
use crate::source::nats::split::NatsSplit;
use crate::source::nats::NatsProperties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SplitReader,
};

pub struct NatsSplitReader {
    subscriber: async_nats::Subscriber,
    properties: NatsProperties,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for NatsSplitReader {
    type Properties = NatsProperties;
    type Split = NatsSplit;

    async fn new(
        properties: NatsProperties,
        splits: Vec<NatsSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // TODO: to simplify the logic, return 1 split for first version
        assert!(splits.len() == 1);
        let subscriber = properties.common.build_subscriber().await?;
        Ok(Self {
            subscriber,
            properties,
            parser_config,
            source_ctx,
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
        #[for_await]
        for msgs in self.subscriber.ready_chunks(capacity) {
            let mut msg_vec = Vec::with_capacity(capacity);
            for msg in msgs {
                msg_vec.push(SourceMessage::from_nats_message(msg));
            }
            yield msg_vec;
        }
    }
}
