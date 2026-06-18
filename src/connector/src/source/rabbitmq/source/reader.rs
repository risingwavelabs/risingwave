// Copyright 2026 RisingWave Labs
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

use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use lapin::options::{BasicConsumeOptions, BasicQosOptions};
use lapin::types::FieldTable;
use lapin::{Connection, Consumer};
use risingwave_common::ensure;
use rw_futures_util::select_all;

use super::message::{RabbitmqMessage, next_ack_consumer_id};
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::common::into_chunk_stream;
use crate::source::rabbitmq::{RabbitmqProperties, RabbitmqSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitMetaData, SplitReader,
};

pub struct RabbitmqSplitReader {
    consumers: Vec<(String, u64, Consumer)>,
    #[expect(dead_code)]
    connection: Option<Connection>,
    split: RabbitmqSplit,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for RabbitmqSplitReader {
    type Properties = RabbitmqProperties;
    type Split = RabbitmqSplit;

    async fn new(
        properties: RabbitmqProperties,
        splits: Vec<RabbitmqSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        ensure!(
            splits.len() == 1,
            "RabbitMQ reader only supports a single split"
        );
        properties.validate()?;
        let split = splits.into_iter().next().unwrap();

        if split.queues().is_empty() {
            return Ok(Self {
                consumers: vec![],
                connection: None,
                split,
                parser_config,
                source_ctx,
            });
        }

        let connection = properties.connect().await?;
        properties.check_queues(&connection, split.queues()).await?;

        let mut consumers = Vec::with_capacity(split.queues().len());
        for queue in split.queues() {
            let ack_consumer_id = next_ack_consumer_id();
            let channel = connection.create_channel().await?;
            channel
                .basic_qos(
                    properties.prefetch_count(),
                    BasicQosOptions { global: false },
                )
                .await?;
            let consumer_tag = format!(
                "{}-{}-{}-{}-{}",
                properties.consumer_tag_prefix(),
                source_ctx.source_id.as_raw_id(),
                source_ctx.actor_id,
                ack_consumer_id,
                queue
            );
            let consumer = channel
                .basic_consume(
                    queue.as_str(),
                    consumer_tag.as_str(),
                    BasicConsumeOptions {
                        no_local: false,
                        no_ack: false,
                        exclusive: false,
                        nowait: false,
                    },
                    FieldTable::default(),
                )
                .await?;
            consumers.push((queue.clone(), ack_consumer_id, consumer));
        }

        Ok(Self {
            consumers,
            connection: Some(connection),
            split,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl RabbitmqSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        if self.consumers.is_empty() {
            std::future::pending::<()>().await;
        }

        let capacity = self.source_ctx.source_ctrl_opts.chunk_size;
        let split_id = self.split.id();
        let streams = self
            .consumers
            .into_iter()
            .map(|(queue, ack_consumer_id, consumer)| {
                consumer.map(move |delivery| {
                    delivery.map(|delivery| (queue.clone(), ack_consumer_id, delivery))
                })
            });
        let mut messages = select_all(streams);

        while let Some(first) = messages.next().await {
            let mut batch = Vec::with_capacity(capacity);
            let (queue, ack_consumer_id, delivery) = first?;
            batch.push(SourceMessage::from(
                RabbitmqMessage::new(
                    split_id.clone(),
                    queue,
                    ack_consumer_id,
                    delivery,
                    &self.source_ctx,
                )
                .await,
            ));

            while batch.len() < capacity {
                match messages.next().now_or_never() {
                    Some(Some(delivery)) => {
                        let (queue, ack_consumer_id, delivery) = delivery?;
                        batch.push(SourceMessage::from(
                            RabbitmqMessage::new(
                                split_id.clone(),
                                queue,
                                ack_consumer_id,
                                delivery,
                                &self.source_ctx,
                            )
                            .await,
                        ));
                    }
                    _ => break,
                }
            }
            yield batch;
        }
    }
}
