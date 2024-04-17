// Copyright 2024 RisingWave Labs
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
use std::usize;

use anyhow::{anyhow, Context};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use google_cloud_pubsub::topic::Topic;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tonic::Status;
use with_options::WithOptions;

use super::catalog::desc::SinkDesc;
use super::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use super::encoder::{
    AvroEncoder, DateHandlingMode, JsonEncoder, RowEncoder, SerTo, TimeHandlingMode,
    TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam};

pub const PUBSUB_SINK: &str = "google_pubsub";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct GooglePubSubConfig {
    /// The Google Pub/Sub Project ID
    #[serde(rename = "pubsub.project_id")]
    pub project_id: String,

    /// Specifies the Pub/Sub topic to publish messages
    #[serde(rename = "pubsub.topic")]
    pub topic: String,

    /// The Google Pub/Sub endpoint URL
    #[serde(rename = "pubsub.endpoint")]
    pub endpoint: String,

    /// use the connector with a pubsub emulator
    /// <https://cloud.google.com/pubsub/docs/emulator>
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    /// A JSON string containing the service account credentials for authorization,
    /// see the [service-account](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) credentials guide.
    /// The provided account credential must have the
    /// `pubsub.publisher` [role](https://cloud.google.com/pubsub/docs/access-control#roles)
    #[serde(rename = "pubsub.credentials")]
    pub credentials: Option<String>,

    // accept "append-only"
    pub r#type: String,
}

impl GooglePubSubConfig {
    fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        serde_json::from_value::<GooglePubSubConfig>(
            serde_json::to_value(values).expect("impossible"),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))
    }

    pub(crate) fn initialize_env(&self) {
        tracing::debug!("setting pubsub environment variables");
        if let Some(emulator_host) = &self.emulator_host {
            std::env::set_var("PUBSUB_EMULATOR_HOST", emulator_host);
        }
        if let Some(credentials) = &self.credentials {
            std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", credentials);
        }
    }
}

// ===

#[derive(Clone, Debug)]
pub struct GooglePubSubSink {
    pub config: GooglePubSubConfig,
    schema: Schema,
    format_desc: SinkFormatDesc,
    name: String,
    is_append_only: bool,
}

impl Sink for GooglePubSubSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<GooglePubSubSinkWriter>;

    const SINK_NAME: &'static str = PUBSUB_SINK;

    fn is_sink_decouple(desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default => Ok(desc.sink_type.is_append_only()),
            SinkDecouple::Disable => Ok(false),
            SinkDecouple::Enable => Ok(true),
        }
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Nats(anyhow!(
                "Google Pub/Sub sink only support append-only mode"
            )));
        }
        if !matches!(self.format_desc.encode, SinkEncode::Json) {
            return Err(SinkError::GooglePubSub(anyhow!(
                "Google Pub/Sub sink only support `Json` sink encode"
            )));
        }

        let conf = &self.config;
        if matches!((&conf.emulator_host, &conf.credentials), (None, None)) {
            return Err(SinkError::GooglePubSub(anyhow!(
                "Configure at least one of `emulator_host` and `credentials` in the Google Pub/Sub sink"
            )));
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            GooglePubSubSinkWriter::new(
                self.config.clone(),
                self.schema.clone(),
                &self.format_desc,
            )
            .await?
            .into_log_sinker(usize::MAX),
        )
    }
}

impl TryFrom<SinkParam> for GooglePubSubSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = GooglePubSubConfig::from_hashmap(param.properties)?;
        Ok(Self {
            config,
            schema,
            name: param.sink_name,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

// ===

struct GooglePubSubPayloadWriter {
    topic: Topic,
    publisher: Publisher,
}

impl GooglePubSubSinkWriter {
    pub async fn new(
        config: GooglePubSubConfig,
        schema: Schema,
        format_desc: &SinkFormatDesc,
    ) -> Result<Self> {
        config.initialize_env();

        let client_config = ClientConfig {
            endpoint: config.endpoint,
            project_id: Some(config.project_id),
            ..Default::default()
        }
        .with_auth()
        .await
        .map_err(|e| SinkError::GooglePubSub(anyhow!(e)))?;
        let client = Client::new(client_config)
            .await
            .map_err(|e| SinkError::GooglePubSub(anyhow!(e)))?;

        let topic = async {
            let topic = client.topic(&config.topic);
            if !topic.exists(None).await? {
                topic.create(None, None).await?;
            }
            Ok(topic)
        }
        .await
        .map_err(|e: Status| SinkError::GooglePubSub(anyhow!(e)))?;

        let timestamptz_mode = TimestamptzHandlingMode::from_options(&format_desc.options)?;
        let encoder = match format_desc.format {
            SinkFormat::AppendOnly => match format_desc.encode {
                SinkEncode::Json => RowEncoderWrapper::Json(JsonEncoder::new(
                    schema,
                    None,
                    DateHandlingMode::FromCe,
                    TimestampHandlingMode::Milli,
                    timestamptz_mode,
                    TimeHandlingMode::Milli,
                )),
                // TODO: support append-only Avro
                // note: update `CONNECTORS_COMPATIBLE_FORMATS`
                // in src/frontend/src/handler/create_sink.rs
                // SinkEncode::Avro => { },
                _ => {
                    return Err(SinkError::Config(anyhow!(
                        "Google Pub/Sub sink encode unsupported: {:?}",
                        format_desc.encode,
                    )))
                }
            },
            _ => {
                return Err(SinkError::Config(anyhow!(
                    "Google Pub/Sub sink only support append-only mode"
                )))
            }
        };

        let publisher = topic.new_publisher(None);
        let payload_writer = GooglePubSubPayloadWriter { topic, publisher };

        Ok(Self {
            payload_writer,
            encoder,
        })
    }
}

pub struct GooglePubSubSinkWriter {
    payload_writer: GooglePubSubPayloadWriter,
    encoder: RowEncoderWrapper,
}

impl AsyncTruncateSinkWriter for GooglePubSubSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        self.payload_writer.write_chunk(chunk, &self.encoder).await
    }
}

impl GooglePubSubPayloadWriter {
    async fn write_chunk(&mut self, chunk: StreamChunk, encoder: &RowEncoderWrapper) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }

            let data = encoder.encode(row)?;

            let msg = PubsubMessage {
                data,
                ..Default::default()
            };
            let awaiter = self.publisher.publish(msg).await;
            _ = awaiter
                .get()
                .await
                .context("Google Pub/Sub sink error")
                .map_err(SinkError::GooglePubSub)?;
        }

        Ok(())
    }
}

// ===

pub enum RowEncoderWrapper {
    Json(JsonEncoder),
    Avro(AvroEncoder),
}

impl RowEncoder for RowEncoderWrapper {
    type Output = Vec<u8>;

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        match self {
            RowEncoderWrapper::Json(json) => json.encode_cols(row, col_indices)?.ser_to(),
            RowEncoderWrapper::Avro(avro) => avro.encode_cols(row, col_indices)?.ser_to(),
        }
    }

    fn schema(&self) -> &Schema {
        match self {
            RowEncoderWrapper::Json(json) => json.schema(),
            RowEncoderWrapper::Avro(avro) => avro.schema(),
        }
    }

    fn col_indices(&self) -> Option<&[usize]> {
        match self {
            RowEncoderWrapper::Json(json) => json.col_indices(),
            RowEncoderWrapper::Avro(avro) => avro.col_indices(),
        }
    }

    fn encode(&self, row: impl Row) -> Result<Self::Output> {
        match self {
            RowEncoderWrapper::Json(json) => json.encode(row)?.ser_to(),
            RowEncoderWrapper::Avro(avro) => avro.encode(row)?.ser_to(),
        }
    }
}
