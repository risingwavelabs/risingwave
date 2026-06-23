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

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionStatus, Consumer};
use moka::future::Cache as MokaCache;
use risingwave_common::ensure;
use rw_futures_util::select_all;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{MissedTickBehavior, interval};
use tokio_stream::wrappers::{IntervalStream, UnboundedReceiverStream};

use super::message::{RabbitmqMessage, next_ack_consumer_id};
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::common::into_chunk_stream;
use crate::source::rabbitmq::{RabbitmqProperties, RabbitmqSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader,
};

/// Pulsar-style live reader mailbox registry for checkpoint acknowledgements.
///
/// This registry only maps an active `ack_consumer_id` to its reader event-loop
/// sender. It must not store `Acker`, deliveries, delivery tags, payloads,
/// offsets, or retry state. Checkpoint ack state is carried exclusively by
/// `SourceMeta::Rabbitmq` and hidden `rabbitmq_ack_data`.
pub static RABBITMQ_ACK_SENDER_REGISTRY: LazyLock<
    MokaCache<u64, UnboundedSender<RabbitmqAckRequest>>,
> = LazyLock::new(|| moka::future::Cache::builder().build());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RabbitmqAckRequest {
    pub ack_consumer_id: u64,
    pub delivery_tag: u64,
}

struct RabbitmqConsumerState {
    queue: String,
    ack_consumer_id: u64,
    channel: Channel,
    consumer: Consumer,
    ack_tx: UnboundedSender<RabbitmqAckRequest>,
    ack_rx: UnboundedReceiver<RabbitmqAckRequest>,
}

struct RabbitmqAckSenderGuard {
    ack_consumer_ids: Vec<u64>,
}

impl Drop for RabbitmqAckSenderGuard {
    fn drop(&mut self) {
        let ack_consumer_ids = std::mem::take(&mut self.ack_consumer_ids);
        if ack_consumer_ids.is_empty() {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                for ack_consumer_id in ack_consumer_ids {
                    RABBITMQ_ACK_SENDER_REGISTRY.remove(&ack_consumer_id).await;
                }
            });
        } else {
            tracing::warn!(
                ack_consumer_count = ack_consumer_ids.len(),
                "RabbitMQ ack sender registry cleanup ran outside a Tokio runtime",
            );
        }
    }
}

pub struct RabbitmqSplitReader {
    consumers: Vec<RabbitmqConsumerState>,
    // Keep the AMQP connection alive for the consumers/channels owned by this reader.
    #[expect(dead_code)]
    connection: Option<Connection>,
    connection_status: Option<ConnectionStatus>,
    blocked_connection_timeout: Duration,
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
                connection_status: None,
                blocked_connection_timeout: properties.blocked_connection_timeout(),
                split,
                parser_config,
                source_ctx,
            });
        }

        let connection = properties.connect().await?;
        let connection_status = connection.status().clone();
        let blocked_connection_timeout = properties.blocked_connection_timeout();
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
            let consumer_tag = rabbitmq_consumer_tag(
                properties.consumer_tag_prefix(),
                source_ctx.source_id.as_raw_id(),
                source_ctx.actor_id,
                ack_consumer_id,
                queue,
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
            let (ack_tx, ack_rx) = unbounded_channel();
            consumers.push(RabbitmqConsumerState {
                queue: queue.clone(),
                ack_consumer_id,
                channel,
                consumer,
                ack_tx,
                ack_rx,
            });
        }

        Ok(Self {
            consumers,
            connection: Some(connection),
            connection_status: Some(connection_status),
            blocked_connection_timeout,
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
    async fn into_data_stream(mut self) {
        if self.consumers.is_empty() {
            std::future::pending::<()>().await;
        }

        let capacity = self.source_ctx.source_ctrl_opts.chunk_size;
        let split_id = self.split.id();
        let mut channels = HashMap::with_capacity(self.consumers.len());
        let mut ack_consumer_ids = Vec::with_capacity(self.consumers.len());
        let mut streams =
            Vec::<BoxStream<'static, crate::error::ConnectorResult<RabbitmqReaderEvent>>>::new();
        for consumer_state in std::mem::take(&mut self.consumers) {
            let RabbitmqConsumerState {
                queue,
                ack_consumer_id,
                channel,
                consumer,
                ack_tx,
                ack_rx,
            } = consumer_state;
            RABBITMQ_ACK_SENDER_REGISTRY
                .entry(ack_consumer_id)
                .and_upsert_with(|_| std::future::ready(ack_tx))
                .await;
            channels.insert(ack_consumer_id, channel);
            ack_consumer_ids.push(ack_consumer_id);
            streams.push(
                consumer
                    .map(move |delivery| {
                        delivery
                            .map(|delivery| RabbitmqReaderEvent::Delivery {
                                queue: queue.clone(),
                                ack_consumer_id,
                                delivery,
                            })
                            .map_err(Into::into)
                    })
                    .boxed(),
            );
            streams.push(
                UnboundedReceiverStream::new(ack_rx)
                    .map(|request| Ok(RabbitmqReaderEvent::Ack(request)))
                    .boxed(),
            );
        }
        let _ack_sender_guard = RabbitmqAckSenderGuard { ack_consumer_ids };
        if self.connection_status.is_some() {
            let mut blocked_ticker =
                interval(blocked_check_interval(self.blocked_connection_timeout));
            blocked_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            streams.push(
                IntervalStream::new(blocked_ticker)
                    .map(|_| Ok(RabbitmqReaderEvent::CheckBlockedConnection))
                    .boxed(),
            );
        }
        let mut events = select_all(streams);
        let mut blocked_since = None;

        while let Some(event) = events.next().await {
            let Some((queue, ack_consumer_id, delivery)) = event?
                .handle(
                    self.connection_status.as_ref(),
                    &channels,
                    &mut blocked_since,
                    self.blocked_connection_timeout,
                    &split_id,
                    &self.source_ctx,
                )
                .await?
            else {
                continue;
            };
            let mut batch = Vec::with_capacity(capacity);
            batch.push(SourceMessage::from(RabbitmqMessage::new(
                split_id.clone(),
                queue,
                ack_consumer_id,
                delivery,
            )));

            while batch.len() < capacity {
                match events.next().now_or_never() {
                    Some(Some(event)) => {
                        let Some((queue, ack_consumer_id, delivery)) = event?
                            .handle(
                                self.connection_status.as_ref(),
                                &channels,
                                &mut blocked_since,
                                self.blocked_connection_timeout,
                                &split_id,
                                &self.source_ctx,
                            )
                            .await?
                        else {
                            continue;
                        };
                        batch.push(SourceMessage::from(RabbitmqMessage::new(
                            split_id.clone(),
                            queue,
                            ack_consumer_id,
                            delivery,
                        )));
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
    Ack(RabbitmqAckRequest),
    CheckBlockedConnection,
}

impl RabbitmqReaderEvent {
    async fn handle(
        self,
        status: Option<&ConnectionStatus>,
        channels: &HashMap<u64, Channel>,
        blocked_since: &mut Option<Instant>,
        timeout: Duration,
        split_id: &SplitId,
        source_ctx: &SourceContextRef,
    ) -> crate::error::ConnectorResult<Option<(String, u64, lapin::message::Delivery)>> {
        match self {
            Self::Delivery {
                queue,
                ack_consumer_id,
                delivery,
            } => Ok(Some((queue, ack_consumer_id, delivery))),
            Self::Ack(request) => {
                if let Some(channel) = channels.get(&request.ack_consumer_id) {
                    // RabbitMQ delivery tags are scoped to the channel that delivered the
                    // message. The checkpoint worker sends the max parser-progress frontier
                    // per ack_consumer_id; cumulative ack is therefore only applied on this
                    // exact reader-owned channel.
                    channel
                        .basic_ack(request.delivery_tag, BasicAckOptions { multiple: true })
                        .await?;
                } else {
                    tracing::debug!(
                        ack_consumer_id = request.ack_consumer_id,
                        delivery_tag = request.delivery_tag,
                        "RabbitMQ ack request skipped because the reader channel is gone",
                    );
                }
                Ok(None)
            }
            Self::CheckBlockedConnection => {
                if let Some(status) = status
                    && let Some(reason) = check_blocked_connection(
                        status,
                        blocked_since,
                        timeout,
                        split_id,
                        source_ctx,
                    )
                {
                    return Err(crate::error::ConnectorError::from(anyhow::anyhow!(reason)));
                }
                Ok(None)
            }
        }
    }
}

fn rabbitmq_consumer_tag(
    prefix: &str,
    source_id: impl Display,
    actor_id: impl Display,
    ack_consumer_id: u64,
    queue: &str,
) -> String {
    let mut hasher = DefaultHasher::new();
    queue.hash(&mut hasher);
    let queue_hash = hasher.finish();
    format!("{prefix}-{source_id}-{actor_id}-{ack_consumer_id}-{queue_hash:016x}")
}

fn blocked_check_interval(timeout: Duration) -> Duration {
    std::cmp::min(
        std::cmp::max(timeout / 10, Duration::from_secs(1)),
        Duration::from_secs(5),
    )
}

fn check_blocked_connection(
    status: &ConnectionStatus,
    blocked_since: &mut Option<Instant>,
    timeout: Duration,
    split_id: &SplitId,
    source_ctx: &SourceContextRef,
) -> Option<String> {
    if status.closed() || status.errored() {
        return None;
    }

    if status.blocked() {
        let since = blocked_since.get_or_insert_with(Instant::now);
        if since.elapsed() >= timeout {
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
            return Some(reason);
        }
    } else {
        *blocked_since = None;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::rabbitmq_consumer_tag;

    #[test]
    fn consumer_tag_hashes_long_queue_name() {
        let queue = "q".repeat(512);
        let tag = rabbitmq_consumer_tag("rw-rabbitmq", 1, 2, 3, &queue);
        assert!(tag.len() < 255);
        assert!(!tag.contains(&queue));
    }
}
