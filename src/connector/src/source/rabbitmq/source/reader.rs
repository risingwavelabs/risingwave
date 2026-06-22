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

use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
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
use tokio::time::{MissedTickBehavior, interval};
use tokio_stream::wrappers::IntervalStream;

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
    connection_status: Option<ConnectionStatus>,
    blocked_connection_timeout: Duration,
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
                connection_status: None,
                blocked_connection_timeout: properties.blocked_connection_timeout(),
                split,
                parser_config,
                source_ctx,
                ack_tracker: RabbitmqAckTracker::default(),
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
            consumers.push((queue.clone(), ack_consumer_id, consumer));
        }

        Ok(Self {
            consumers,
            connection: Some(connection),
            connection_status: Some(connection_status),
            blocked_connection_timeout,
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
            let Some((queue, ack_consumer_id, delivery)) = event?.into_delivery(
                self.connection_status.as_ref(),
                &mut blocked_since,
                self.blocked_connection_timeout,
                &split_id,
                &self.source_ctx,
            )?
            else {
                continue;
            };
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
                match events.next().now_or_never() {
                    Some(Some(event)) => {
                        let Some((queue, ack_consumer_id, delivery)) = event?.into_delivery(
                            self.connection_status.as_ref(),
                            &mut blocked_since,
                            self.blocked_connection_timeout,
                            &split_id,
                            &self.source_ctx,
                        )?
                        else {
                            continue;
                        };
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
    CheckBlockedConnection,
}

impl RabbitmqReaderEvent {
    fn into_delivery(
        self,
        status: Option<&ConnectionStatus>,
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
