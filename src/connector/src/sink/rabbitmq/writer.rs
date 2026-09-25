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

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, anyhow};
use futures::FutureExt;
use futures::future::BoxFuture;
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Confirmation};
use risingwave_common::array::{Op, StreamChunk};
use tokio::time::{Instant, timeout, timeout_at};

use super::RabbitMqConfig;
use super::client::RabbitMqClient;
use super::encoder::RabbitMqEncoder;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::AsyncTruncateSinkWriter;
use crate::sink::{Result, SinkError};

const PERSISTENT_DELIVERY_MODE: u8 = 2;

pub type RabbitMqDeliveryFuture = BoxFuture<'static, Result<()>>;

/// Publishes append-only rows and lets the log sinker truncate only confirmed chunks.
pub struct RabbitMqSinkWriter {
    client: RabbitMqClient,
    config: RabbitMqConfig,
    encoder: RabbitMqEncoder,
}

impl RabbitMqSinkWriter {
    pub async fn new(config: RabbitMqConfig, encoder: impl Into<RabbitMqEncoder>) -> Result<Self> {
        let client = RabbitMqClient::connect(&config).await?;
        Ok(Self {
            client,
            config,
            encoder: encoder.into(),
        })
    }
}

impl AsyncTruncateSinkWriter for RabbitMqSinkWriter {
    type DeliveryFuture = RabbitMqDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let channel = &self.client.channel;
        let exchange = &self.config.exchange;
        let routing_key = &self.config.routing_key;
        publish_chunk(
            &self.config,
            &self.encoder,
            chunk,
            add_future,
            |payload, options, properties| async move {
                channel
                    .basic_publish(
                        exchange.as_str().into(),
                        routing_key.as_str().into(),
                        options,
                        &payload,
                        properties,
                    )
                    .await
            },
        )
        .await
    }
}

// Keep publishing injectable so that delivery and backpressure tests can use controlled
// confirmations without a broker. Both production and tests run the same chunk loop.
async fn publish_chunk<P, PF, CF>(
    config: &RabbitMqConfig,
    encoder: &RabbitMqEncoder,
    chunk: StreamChunk,
    mut add_future: DeliveryFutureManagerAddFuture<'_, RabbitMqDeliveryFuture>,
    mut publish: P,
) -> Result<()>
where
    P: FnMut(Vec<u8>, BasicPublishOptions, BasicProperties) -> PF + Send,
    PF: Future<Output = lapin::Result<CF>> + Send,
    CF: Future<Output = lapin::Result<Confirmation>> + Send + 'static,
{
    if chunk.rows().any(|(op, _)| op != Op::Insert) {
        return Err(SinkError::RabbitMq(anyhow!(
            "RabbitMQ sink only supports insert operations"
        )));
    }

    let max_inflight = config
        .max_inflight_messages
        .min(add_future.max_future_count());
    for (_, row) in chunk.rows() {
        // Reserve capacity before publishing; adding the future afterwards alone
        // would allow one extra message to be submitted when the window is full.
        while add_future.future_count() >= max_inflight {
            add_future.await_one_delivery().await?;
        }

        let payload = encoder.encode(row)?;
        if payload.len() > config.max_message_size {
            return Err(SinkError::RabbitMq(anyhow!(
                "RabbitMQ encoded message size {} exceeds max_message_size {}",
                payload.len(),
                config.max_message_size
            )));
        }

        let confirm = timeout(
            Duration::from_millis(config.publish_timeout_ms),
            publish(
                payload,
                BasicPublishOptions {
                    mandatory: true,
                    immediate: false,
                },
                BasicProperties::default()
                    .with_content_type(encoder.content_type().into())
                    .with_delivery_mode(PERSISTENT_DELIVERY_MODE),
            ),
        )
        .await
        .context("RabbitMQ publish submission timed out")
        .and_then(|result| result.context("failed to publish RabbitMQ message"))
        .map_err(SinkError::RabbitMq)?;

        // Capture the deadline before enqueueing: the manager may not poll this
        // future until earlier messages finish. Queueing must not reset its timeout.
        let deadline = Instant::now() + Duration::from_millis(config.confirm_timeout_ms);
        add_future
            .add_future_may_await(delivery_future(confirm, deadline))
            .await?;
    }
    Ok(())
}

fn delivery_future(
    confirm: impl Future<Output = lapin::Result<Confirmation>> + Send + 'static,
    deadline: Instant,
) -> RabbitMqDeliveryFuture {
    async move {
        let confirmation = timeout_at(deadline, confirm)
            .await
            .context("RabbitMQ publisher confirm timed out")
            .and_then(|result| result.context("failed to confirm RabbitMQ message"))
            .map_err(SinkError::RabbitMq)?;
        match confirmation {
            Confirmation::Ack(None) => Ok(()),
            Confirmation::Ack(Some(returned)) => Err(SinkError::RabbitMq(anyhow!(
                "RabbitMQ message was returned (reply code {}): {}",
                returned.reply_code,
                returned.reply_text
            ))),
            Confirmation::Nack(_) => Err(SinkError::RabbitMq(anyhow!(
                "RabbitMQ broker negatively acknowledged the message"
            ))),
            Confirmation::NotRequested => Err(SinkError::RabbitMq(anyhow!(
                "RabbitMQ publisher confirms are not enabled"
            ))),
        }
    }
    .boxed()
}

#[cfg(test)]
mod tests;
