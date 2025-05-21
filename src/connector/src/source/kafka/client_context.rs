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

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Weak};

use anyhow::anyhow;
use aws_config::Region;
use aws_sdk_s3::config::SharedCredentialsProvider;
use rdkafka::client::{BrokerAddr, OAuthToken};
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::ProducerContext;
use rdkafka::{ClientContext, Statistics};
use tokio::runtime::Runtime;
use tokio::time::interval;

use super::private_link::{BrokerAddrRewriter, PrivateLinkContextRole};
use super::stats::RdKafkaStats;
use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;
use crate::Duration;

struct IamAuthEnv {
    credentials_provider: SharedCredentialsProvider,
    region: Region,
    // XXX(runji): madsim does not support `Handle` for now
    #[cfg(not(madsim))]
    rt: tokio::runtime::Handle,
}

pub struct KafkaContextCommon {
    // For VPC PrivateLink support
    addr_rewriter: BrokerAddrRewriter,

    // identifier is required when reporting metrics as a label, usually it is compose by connector
    // format (source or sink) and corresponding id (source_id or sink_id)
    // identifier and metrics should be set at the same time
    identifier: Option<String>,
    metrics: Option<Arc<RdKafkaStats>>,

    /// Credential and region for AWS MSK
    auth: Option<IamAuthEnv>,
}

pub fn spawn_consumer_poll_task(weak_consumer: Weak<StreamConsumer<RwConsumerContext>>) {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60));
        loop {
            if let Some(consumer) = weak_consumer.upgrade() {
                let _ = tokio::task::spawn_blocking(move || unsafe {
                    rdkafka::bindings::rd_kafka_poll(consumer.client().native_ptr(), 0)
                })
                .await;
            } else {
                break;
            }
            interval.tick().await;
        }
    });
}

impl KafkaContextCommon {
    pub async fn new(
        broker_rewrite_map: Option<BTreeMap<String, String>>,
        identifier: Option<String>,
        metrics: Option<Arc<RdKafkaStats>>,
        auth: AwsAuthProps,
        is_aws_msk_iam: bool,
    ) -> ConnectorResult<Self> {
        let addr_rewriter =
            BrokerAddrRewriter::new(PrivateLinkContextRole::Consumer, broker_rewrite_map)?;
        let auth = if is_aws_msk_iam {
            let config = auth.build_config().await?;
            let credentials_provider = config
                .credentials_provider()
                .ok_or_else(|| anyhow!("missing aws credentials_provider"))?;
            let region = config
                .region()
                .ok_or_else(|| anyhow!("missing aws region"))?
                .clone();
            Some(IamAuthEnv {
                credentials_provider,
                region,
                #[cfg(not(madsim))]
                rt: tokio::runtime::Handle::current(),
            })
        } else {
            None
        };
        Ok(Self {
            addr_rewriter,
            identifier,
            metrics,
            auth,
        })
    }
}

pub static KAFKA_SOURCE_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("rw-frontend")
        .enable_all()
        .build()
        .expect("failed to build frontend runtime")
});

impl KafkaContextCommon {
    fn stats(&self, statistics: Statistics) {
        if let Some(metrics) = &self.metrics
            && let Some(id) = &self.identifier
        {
            metrics.report(id.as_str(), &statistics);
        }
    }

    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        self.addr_rewriter.rewrite_broker_addr(addr)
    }

    // XXX(runji): oauth is ignored in simulation
    #[cfg_or_panic::cfg_or_panic(not(madsim))]
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
        use tokio::time::{timeout, Duration};

        if let Some(IamAuthEnv {
            credentials_provider,
            region,
            ..
        }) = &self.auth
        {
            let region = region.clone();
            let credentials_provider = credentials_provider.clone();
            let (token, expiration_time_ms) = {
                tokio::task::block_in_place(move || {
                    KAFKA_SOURCE_RUNTIME.block_on(async {
                        timeout(
                            Duration::from_secs(10),
                            generate_auth_token_from_credentials_provider(
                                region,
                                credentials_provider,
                            ),
                        )
                        .await
                    })
                })
                .unwrap()
                .unwrap()
            };
            tracing::info!(?expiration_time_ms, "generated token");
            Ok(OAuthToken {
                token,
                principal_name: "".to_owned(),
                lifetime_ms: expiration_time_ms,
            })
        } else {
            Err("must provide AWS IAM credential".into())
        }
    }

    fn enable_refresh_oauth_token(&self) -> bool {
        self.auth.is_some()
    }
}

pub type BoxConsumerContext = Box<dyn ConsumerContext>;

/// Kafka consumer context used for private link, IAM auth, and metrics
pub struct RwConsumerContext {
    common: KafkaContextCommon,
}

impl RwConsumerContext {
    pub fn new(common: KafkaContextCommon) -> Self {
        Self { common }
    }
}

impl ClientContext for RwConsumerContext {
    /// this func serves as a callback when `poll` is completed.
    fn stats(&self, statistics: Statistics) {
        self.common.stats(statistics);
    }

    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        self.common.rewrite_broker_addr(addr)
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        self.common.generate_oauth_token(oauthbearer_config)
    }

    fn enable_refresh_oauth_token(&self) -> bool {
        self.common.enable_refresh_oauth_token()
    }
}

// required by the trait bound of BaseConsumer
impl ConsumerContext for RwConsumerContext {}

/// Kafka producer context used for private link, IAM auth, and metrics
pub struct RwProducerContext {
    common: KafkaContextCommon,
}

impl RwProducerContext {
    pub fn new(common: KafkaContextCommon) -> Self {
        Self { common }
    }
}

impl ClientContext for RwProducerContext {
    fn stats(&self, statistics: Statistics) {
        self.common.stats(statistics);
    }

    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        self.common.rewrite_broker_addr(addr)
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        self.common.generate_oauth_token(oauthbearer_config)
    }

    fn enable_refresh_oauth_token(&self) -> bool {
        self.common.enable_refresh_oauth_token()
    }
}

impl ProducerContext for RwProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}
