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
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;

use crate::impl_common_split_reader_logic;
use crate::parser::ParserConfig;
use crate::source::nats::NatsProperties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SplitImpl, SplitReader,
};

impl_common_split_reader_logic!(NatsSplitReader, NatsProperties);

pub struct NatsSplitReader {
    subscriber: async_nats::Subscriber,
    properties: NatsProperties,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
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
        let subscriber = properties.common.build_subscriber().await?;
        Ok(Self {
            subscriber,
            properties,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}

impl NatsSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        // TODO: how to deal with msg by batch
        let mut subscriber = self.subscriber;
        while let Some(message) = subscriber.next().await {
            yield vec![SourceMessage::from_nats_message(message)]
        }

        // let mut msgs = Vec::with_capacity(1024);
        // while let Some(message) = subscriber.next().await {
        //     msgs.push(SourceMessage::from_nats_message(message));
        //     if msgs.len() >= 1024 {
        //         yield msgs;
        //         msgs = Vec::with_capacity(1024);
        //     }
        // }
        // yield msgs;
    }
}
