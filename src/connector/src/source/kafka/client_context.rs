use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use anyhow::anyhow;
use aws_config::Region;
use aws_sdk_s3::config::SharedCredentialsProvider;
use rdkafka::client::{BrokerAddr, OAuthToken};
use rdkafka::consumer::ConsumerContext;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::ProducerContext;
use rdkafka::{ClientContext, Statistics};
use tokio::runtime::Handle;

use super::private_link::{BrokerAddrRewriter, PrivateLinkContextRole};
use super::stats::RdKafkaStats;
use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;

struct IamAuthEnv {
    credentials_provider: SharedCredentialsProvider,
    region: Region,
    rt: Handle,
}

#[derive()]
pub struct KafkaContextCommon {
    addr_rewriter: BrokerAddrRewriter,

    // identifier is required when reporting metrics as a label, usually it is compose by connector
    // format (source or sink) and corresponding id (source_id or sink_id)
    // identifier and metrics should be set at the same time
    identifier: Option<String>,
    metrics: Option<Arc<RdKafkaStats>>,

    /// Credential and region for AWS MSK
    auth: Option<IamAuthEnv>,
}

impl KafkaContextCommon {
    pub async fn new(
        broker_rewrite_map: Option<HashMap<String, String>>,
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
                rt: Handle::current(),
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

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
        use tokio::time::{timeout, Duration};

        if let Some(IamAuthEnv {
            credentials_provider,
            region,
            rt,
        }) = &self.auth
        {
            let region = region.clone();
            let credentials_provider = credentials_provider.clone();
            let rt = rt.clone();
            let (token, expiration_time_ms) = {
                let handle = thread::spawn(move || {
                    rt.block_on(async {
                        timeout(
                            Duration::from_secs(10),
                            generate_auth_token_from_credentials_provider(
                                region,
                                credentials_provider,
                            ),
                        )
                        .await
                    })
                });
                handle.join().unwrap()??
            };
            Ok(OAuthToken {
                token,
                principal_name: "".to_string(),
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
