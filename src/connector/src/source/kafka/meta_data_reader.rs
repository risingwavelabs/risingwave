// Copyright 2025 RisingWave Labs
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

use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use moka::future::Cache as MokaCache;
use moka::ops::compute::Op;
use rdkafka::ClientConfig;
use rdkafka::consumer::BaseConsumer;

use super::{KAFKA_ISOLATION_LEVEL, KafkaContextCommon, RwConsumerContext};
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::kafka::{KafkaConnectionProps, KafkaProperties};

pub type KafkaMetaConsumer = BaseConsumer<RwConsumerContext>;

pub static INSTANCE: LazyLock<MokaCache<KafkaConnectionProps, PolledKafkaMetaConsumer>> =
    LazyLock::new(|| moka::future::Cache::builder().build());

pub struct SharedKafkaMetaClient;

impl SharedKafkaMetaClient {
    pub async fn get_client(
        properties: &KafkaProperties,
    ) -> ConnectorResult<Arc<KafkaMetaConsumer>> {
        let mut config = rdkafka::ClientConfig::new();
        let broker_address = properties.connection.brokers.clone();
        config.set("bootstrap.servers", &broker_address);
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        properties.connection.set_security_properties(&mut config);
        properties.set_client(&mut config);

        let mut client: Option<Arc<KafkaMetaConsumer>> = None;

        INSTANCE
            .entry_by_ref(&properties.connection)
            .and_try_compute_with::<_, _, ConnectorError>(|maybe_entry| async {
                if let Some(entry) = maybe_entry {
                    let entry_value = entry.into_value();
                    if let Some(client_) = entry_value.inner.clone().upgrade() {
                        // return if the client is already built
                        tracing::info!("reuse existing kafka client for {}", broker_address);
                        client = Some(client_);
                        return Ok(Op::Nop);
                    }
                }
                tracing::info!("build new kafka client for {}", broker_address);
                let consumer = build_kafka_client(&config, properties).await?;
                let weak_consumer = Arc::downgrade(&consumer);
                let polled_consumer = PolledKafkaMetaConsumer::new(weak_consumer, properties);
                client = Some(consumer);
                Ok(Op::Put(polled_consumer))
            })
            .await?;
        Ok(client.unwrap())
    }
}

#[derive(Clone)]
pub struct PolledKafkaMetaConsumer {
    inner: Weak<BaseConsumer<RwConsumerContext>>,
    /// Polling handle for refreshing oauth token
    /// Use `Arc` because `MokaCache` requires `Clone` on the value
    poll_handle: Arc<Option<tokio::task::JoinHandle<()>>>,
    /// The broker address for logging purposes
    broker_addr: String,
}

impl PolledKafkaMetaConsumer {
    fn new(inner: Weak<BaseConsumer<RwConsumerContext>>, properties: &KafkaProperties) -> Self {
        let consumer_to_poll = inner.clone();
        let poll_handle = if properties.connection.is_aws_msk_iam() {
            let broker_addr = properties.connection.brokers.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    if let Some(consumer) = consumer_to_poll.upgrade() {
                        // Note that before any SASL/OAUTHBEARER broker connection can succeed the application must call
                        // rd_kafka_oauthbearer_set_token() once – either directly or, more typically, by invoking either
                        // rd_kafka_poll(), rd_kafka_consumer_poll(), rd_kafka_queue_poll(), etc, in order to cause retrieval
                        // of an initial token to occur.
                        // https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a988395722598f63396d7a1bedb22adaf
                        #[cfg(not(madsim))]
                        consumer.poll(Duration::from_secs(10)); // note: this is a blocking call
                        #[cfg(madsim)]
                        consumer.poll(Duration::from_secs(10)).await;
                    } else {
                        tracing::info!(
                            "shared kafka meta data client for {} is dropped, stop polling",
                            broker_addr
                        );
                        break;
                    }
                }
            }))
        } else {
            None
        };
        Self {
            inner,
            poll_handle: Arc::new(poll_handle),
            broker_addr: properties.connection.brokers.clone(),
        }
    }
}

impl Drop for PolledKafkaMetaConsumer {
    fn drop(&mut self) {
        if let Some(handle) = self.poll_handle.as_ref() {
            handle.abort();
            tracing::info!(
                "shared kafka meta data client for {} is dropped, poll handle aborted",
                self.broker_addr
            );
        }
    }
}

async fn build_kafka_client(
    config: &ClientConfig,
    properties: &KafkaProperties,
) -> ConnectorResult<Arc<KafkaMetaConsumer>> {
    let ctx_common = KafkaContextCommon::new(
        properties.privatelink_common.broker_rewrite_map.clone(),
        None,
        None,
        properties.aws_auth_props.clone(),
        properties.connection.is_aws_msk_iam(),
    )
    .await?;
    let client_ctx = RwConsumerContext::new(ctx_common);
    let client: KafkaMetaConsumer = config.create_with_context(client_ctx).await?;
    // Initial poll to trigger the oauth token refresh
    #[cfg(not(madsim))]
    client.poll(Duration::from_secs(10)); // note: this is a blocking call
    #[cfg(madsim)]
    client.poll(Duration::from_secs(10)).await;
    Ok(Arc::new(client))
}
