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

use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use lapin::options::{BasicConsumeOptions, BasicQosOptions};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionStatus, Consumer};
use risingwave_common::ensure;
use rw_futures_util::select_all;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::message::{RabbitmqAckTracker, RabbitmqMessage, next_ack_consumer_id};
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::common::into_chunk_stream;
use crate::source::rabbitmq::{RabbitmqProperties, RabbitmqSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader,
};

pub struct RabbitmqSplitReader {
    consumers: Vec<(String, u64, Consumer)>,
    // Keep the AMQP connection alive for the consumers/channels owned by this reader.
    #[expect(dead_code)]
    connection: Option<Connection>,
    blocked_monitor: Option<JoinHandle<()>>,
    blocked_timeout_rx: Option<mpsc::UnboundedReceiver<String>>,
    split: RabbitmqSplit,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    ack_tracker: RabbitmqAckTracker,
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
                blocked_monitor: None,
                blocked_timeout_rx: None,
                split,
                parser_config,
                source_ctx,
                ack_tracker: RabbitmqAckTracker::default(),
            });
        }

        let connection = properties.connect().await?;
        let (blocked_monitor, blocked_timeout_rx) = spawn_blocked_connection_monitor(
            connection.status().clone(),
            properties.blocked_connection_timeout(),
            split.id(),
            source_ctx.clone(),
        );
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
            blocked_monitor: Some(blocked_monitor),
            blocked_timeout_rx: Some(blocked_timeout_rx),
            split,
            parser_config,
            source_ctx,
            ack_tracker: RabbitmqAckTracker::default(),
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
    async fn into_data_stream(mut self) {
        if self.consumers.is_empty() {
            std::future::pending::<()>().await;
        }

        let capacity = self.source_ctx.source_ctrl_opts.chunk_size;
        let split_id = self.split.id();
        let streams = std::mem::take(&mut self.consumers).into_iter().map(
            |(queue, ack_consumer_id, consumer)| {
                consumer.map(move |delivery| {
                    delivery
                        .map(|delivery| RabbitmqReaderEvent::Delivery {
                            queue: queue.clone(),
                            ack_consumer_id,
                            delivery,
                        })
                        .map_err(Into::into)
                })
            },
        );
        let mut streams = streams
            .map(|stream| stream.boxed())
            .collect::<Vec<BoxStream<'static, crate::error::ConnectorResult<RabbitmqReaderEvent>>>>(
            );
        if let Some(blocked_timeout_rx) = self.blocked_timeout_rx.take() {
            streams.push(
                UnboundedReceiverStream::new(blocked_timeout_rx)
                    .map(|reason| Ok(RabbitmqReaderEvent::BlockedTimeout(reason)))
                    .boxed(),
            );
        }
        let mut messages = select_all(streams);

        while let Some(first) = messages.next().await {
            let (queue, ack_consumer_id, delivery) = first?.into_delivery()?;
            let mut batch = Vec::with_capacity(capacity);
            batch.push(SourceMessage::from(
                RabbitmqMessage::new(
                    split_id.clone(),
                    queue,
                    ack_consumer_id,
                    &self.ack_tracker,
                    delivery,
                    &self.source_ctx,
                )
                .await,
            ));

            while batch.len() < capacity {
                match messages.next().now_or_never() {
                    Some(Some(event)) => {
                        let (queue, ack_consumer_id, delivery) = event?.into_delivery()?;
                        batch.push(SourceMessage::from(
                            RabbitmqMessage::new(
                                split_id.clone(),
                                queue,
                                ack_consumer_id,
                                &self.ack_tracker,
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

enum RabbitmqReaderEvent {
    Delivery {
        queue: String,
        ack_consumer_id: u64,
        delivery: lapin::message::Delivery,
    },
    BlockedTimeout(String),
}

impl RabbitmqReaderEvent {
    fn into_delivery(
        self,
    ) -> crate::error::ConnectorResult<(String, u64, lapin::message::Delivery)> {
        match self {
            Self::Delivery {
                queue,
                ack_consumer_id,
                delivery,
            } => Ok((queue, ack_consumer_id, delivery)),
            Self::BlockedTimeout(reason) => Err(crate::error::ConnectorError::from(
                anyhow::anyhow!("{}", reason),
            )),
        }
    }
}

impl Drop for RabbitmqSplitReader {
    fn drop(&mut self) {
        if let Some(blocked_monitor) = self.blocked_monitor.take() {
            blocked_monitor.abort();
        }
    }
}

fn spawn_blocked_connection_monitor(
    status: ConnectionStatus,
    timeout: Duration,
    split_id: SplitId,
    source_ctx: SourceContextRef,
) -> (JoinHandle<()>, mpsc::UnboundedReceiver<String>) {
    let (timeout_tx, timeout_rx) = mpsc::unbounded_channel();
    let check_interval = std::cmp::min(
        std::cmp::max(timeout / 10, Duration::from_secs(1)),
        Duration::from_secs(5),
    );
    let handle = tokio::spawn(async move {
        let mut ticker = interval(check_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut blocked_since = None;

        loop {
            ticker.tick().await;
            if status.closed() || status.errored() {
                return;
            }

            if status.blocked() {
                let blocked_since = blocked_since.get_or_insert_with(Instant::now);
                if blocked_since.elapsed() >= timeout {
                    let reason = format!(
                        "RabbitMQ connection for split `{split_id}` stayed blocked for {timeout:?}"
                    );
                    tracing::error!(
                        source_id = source_ctx.source_id.as_raw_id(),
                        source_name = source_ctx.source_name,
                        actor_id = %source_ctx.actor_id,
                        fragment_id = %source_ctx.fragment_id,
                        %split_id,
                        ?timeout,
                        "RabbitMQ connection blocked timeout exceeded; failing reader so unacked deliveries are requeued on connection drop",
                    );
                    let _ = timeout_tx.send(reason);
                    return;
                }
            } else {
                blocked_since = None;
            }
        }
    });
    (handle, timeout_rx)
}
