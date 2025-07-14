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

pub mod prelude {
    // import all split enumerators
    pub use crate::source::datagen::DatagenSplitEnumerator;
    pub use crate::source::filesystem::LegacyS3SplitEnumerator;
    pub use crate::source::filesystem::opendal_source::OpendalEnumerator;
    pub use crate::source::google_pubsub::PubsubSplitEnumerator as GooglePubsubSplitEnumerator;
    pub use crate::source::iceberg::IcebergSplitEnumerator;
    pub use crate::source::kafka::KafkaSplitEnumerator;
    pub use crate::source::kinesis::KinesisSplitEnumerator;
    pub use crate::source::mqtt::MqttSplitEnumerator;
    pub use crate::source::nats::NatsSplitEnumerator;
    pub use crate::source::nexmark::NexmarkSplitEnumerator;
    pub use crate::source::pulsar::PulsarSplitEnumerator;
    pub use crate::source::test_source::TestSourceSplitEnumerator as TestSplitEnumerator;
    pub type AzblobSplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalAzblob>;
    pub type GcsSplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalGcs>;
    pub type OpendalS3SplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalS3>;
    pub type PosixFsSplitEnumerator =
        OpendalEnumerator<crate::source::filesystem::opendal_source::OpendalPosixFs>;
    pub use crate::source::cdc::enumerator::DebeziumSplitEnumerator;
    pub type CitusCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Citus>;
    pub type MongodbCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Mongodb>;
    pub type PostgresCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Postgres>;
    pub type MysqlCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::Mysql>;
    pub type SqlServerCdcSplitEnumerator = DebeziumSplitEnumerator<crate::source::cdc::SqlServer>;
}

pub mod base;
pub mod cdc;
pub mod data_gen_util;
pub mod datagen;
pub mod filesystem;
pub mod google_pubsub;
pub mod kafka;
pub mod kinesis;
pub mod monitor;
pub mod mqtt;
pub mod nats;
pub mod nexmark;
pub mod pulsar;

mod batch;
mod util;
use std::future::IntoFuture;

pub use base::{UPSTREAM_SOURCE_KEY, WEBHOOK_CONNECTOR, *};
pub use batch::BatchSourceSplitImpl;
pub(crate) use common::*;
use google_cloud_pubsub::subscription::Subscription;
pub use google_pubsub::GOOGLE_PUBSUB_CONNECTOR;
pub use kafka::KAFKA_CONNECTOR;
pub use kinesis::KINESIS_CONNECTOR;
pub use mqtt::MQTT_CONNECTOR;
pub use nats::NATS_CONNECTOR;
mod common;
pub mod iceberg;
mod manager;
pub mod reader;
pub mod test_source;

use async_nats::jetstream::consumer::AckPolicy as JetStreamAckPolicy;
use async_nats::jetstream::context::Context as JetStreamContext;
pub use manager::{SourceColumnDesc, SourceColumnType};
use risingwave_common::array::{Array, ArrayRef};
use thiserror_ext::AsReport;
pub use util::fill_adaptive_split;

pub use crate::source::filesystem::LEGACY_S3_CONNECTOR;
pub use crate::source::filesystem::opendal_source::{
    AZBLOB_CONNECTOR, GCS_CONNECTOR, OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR,
};
pub use crate::source::nexmark::NEXMARK_CONNECTOR;
pub use crate::source::pulsar::PULSAR_CONNECTOR;

pub fn should_copy_to_format_encode_options(key: &str, connector: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "schema.registry",
        "schema.location",
        "message",
        "key.message",
        "without_header",
        "delimiter",
        // AwsAuthProps
        "region",
        "endpoint_url",
        "access_key",
        "secret_key",
        "session_token",
        "arn",
        "external_id",
        "profile",
    ];
    PREFIXES.iter().any(|prefix| key.starts_with(prefix))
        || (key == "endpoint" && !connector.eq_ignore_ascii_case(KINESIS_CONNECTOR))
}

/// Tasks executed by `WaitCheckpointWorker`
pub enum WaitCheckpointTask {
    CommitCdcOffset(Option<(SplitId, String)>),
    AckPubsubMessage(Subscription, Vec<ArrayRef>),
    AckNatsJetStream(JetStreamContext, Vec<ArrayRef>, JetStreamAckPolicy),
}

impl WaitCheckpointTask {
    pub async fn run(self) {
        use std::str::FromStr;
        match self {
            WaitCheckpointTask::CommitCdcOffset(updated_offset) => {
                if let Some((split_id, offset)) = updated_offset {
                    let source_id: u64 = u64::from_str(split_id.as_ref()).unwrap();
                    // notify cdc connector to commit offset
                    match cdc::jni_source::commit_cdc_offset(source_id, offset.clone()) {
                        Ok(()) => {}
                        Err(e) => {
                            tracing::error!(
                                error = %e.as_report(),
                                "source#{source_id}: failed to commit cdc offset: {offset}.",
                            )
                        }
                    }
                }
            }
            WaitCheckpointTask::AckPubsubMessage(subscription, ack_id_arrs) => {
                async fn ack(subscription: &Subscription, ack_ids: Vec<String>) {
                    tracing::trace!("acking pubsub messages {:?}", ack_ids);
                    match subscription.ack(ack_ids).await {
                        Ok(()) => {}
                        Err(e) => {
                            tracing::error!(
                                error = %e.as_report(),
                                "failed to ack pubsub messages",
                            )
                        }
                    }
                }
                const MAX_ACK_BATCH_SIZE: usize = 1000;
                let mut ack_ids: Vec<String> = vec![];
                for arr in ack_id_arrs {
                    for ack_id in arr.as_utf8().iter().flatten() {
                        ack_ids.push(ack_id.to_owned());
                        if ack_ids.len() >= MAX_ACK_BATCH_SIZE {
                            ack(&subscription, std::mem::take(&mut ack_ids)).await;
                        }
                    }
                }
                ack(&subscription, ack_ids).await;
            }
            WaitCheckpointTask::AckNatsJetStream(
                ref context,
                reply_subjects_arrs,
                ref ack_policy,
            ) => {
                async fn ack(context: &JetStreamContext, reply_subject: String) {
                    match context.publish(reply_subject.clone(), "+ACK".into()).await {
                        Err(e) => {
                            tracing::error!(error = %e.as_report(), subject = ?reply_subject, "failed to ack NATS JetStream message");
                        }
                        Ok(ack_future) => {
                            let _ = ack_future.into_future().await;
                        }
                    }
                }

                let reply_subjects = reply_subjects_arrs
                    .iter()
                    .flat_map(|arr| {
                        arr.as_utf8()
                            .iter()
                            .flatten()
                            .map(|s| s.to_owned())
                            .collect::<Vec<String>>()
                    })
                    .collect::<Vec<String>>();

                match ack_policy {
                    JetStreamAckPolicy::None => (),
                    JetStreamAckPolicy::Explicit => {
                        for reply_subject in reply_subjects {
                            if reply_subject.is_empty() {
                                continue;
                            }
                            ack(context, reply_subject).await;
                        }
                    }
                    JetStreamAckPolicy::All => {
                        if let Some(reply_subject) = reply_subjects.last() {
                            ack(context, reply_subject.clone()).await;
                        }
                    }
                }
            }
        }
    }
}
