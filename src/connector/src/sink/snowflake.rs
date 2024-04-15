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
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_object_store::object::{ObjectStore, StreamingUploader};
use serde::Deserialize;
use serde_json::Value;
use serde_with::serde_as;
use uuid::Uuid;
use with_options::WithOptions;

use super::encoder::{
    JsonEncoder, RowEncoder, TimeHandlingMode, TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::snowflake_connector::{generate_s3_file_name, SnowflakeHttpClient, SnowflakeS3Client};
use super::writer::LogSinkerOf;
use super::{SinkError, SinkParam};
use crate::sink::writer::SinkWriterExt;
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

pub const SNOWFLAKE_SINK: &str = "snowflake";

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct SnowflakeCommon {
    /// The snowflake database used for sinking
    #[serde(rename = "snowflake.database")]
    pub database: String,

    /// The corresponding schema where sink table exists
    #[serde(rename = "snowflake.schema")]
    pub schema: String,

    /// The created pipe object, will be used as `insertFiles` target
    #[serde(rename = "snowflake.pipe")]
    pub pipe: String,

    /// The unique, snowflake provided `account_identifier`
    /// NOTE: please use the form `<orgname>-<account_name>`
    /// For detailed guidance, reference: <https://docs.snowflake.com/en/user-guide/admin-account-identifier>
    #[serde(rename = "snowflake.account_identifier")]
    pub account_identifier: String,

    /// The user that owns the table to be sinked
    /// NOTE: the user should've been granted corresponding *role*
    /// reference: <https://docs.snowflake.com/en/sql-reference/sql/grant-role>
    #[serde(rename = "snowflake.user")]
    pub user: String,

    /// The public key fingerprint used when generating custom `jwt_token`
    /// reference: <https://docs.snowflake.com/en/developer-guide/sql-api/authenticating>
    #[serde(rename = "snowflake.rsa_public_key_fp")]
    pub rsa_public_key_fp: String,

    /// The rsa pem key *without* encryption
    #[serde(rename = "snowflake.private_key")]
    pub private_key: String,

    /// The s3 bucket where intermediate sink files will be stored
    #[serde(rename = "snowflake.s3_bucket")]
    pub s3_bucket: String,

    /// The optional s3 path to be specified
    /// the actual file location would be `<s3_bucket>://<s3_path>/<rw_auto_gen_file_name>`
    /// if this field is specified by user(s)
    /// otherwise it would be `<s3_bucket>://<rw_auto_gen_file_name>`
    #[serde(rename = "snowflake.s3_path")]
    pub s3_path: Option<String>,

    /// s3 credentials
    #[serde(rename = "snowflake.aws_access_key_id")]
    pub aws_access_key_id: String,

    /// s3 credentials
    #[serde(rename = "snowflake.aws_secret_access_key")]
    pub aws_secret_access_key: String,

    /// The s3 region, e.g., us-east-2
    #[serde(rename = "snowflake.aws_region")]
    pub aws_region: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(flatten)]
    pub common: SnowflakeCommon,
}

impl SnowflakeConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SnowflakeConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct SnowflakeSink {
    pub config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl Sink for SnowflakeSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<SnowflakeSinkWriter>;

    const SINK_NAME: &'static str = SNOWFLAKE_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(SnowflakeSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Config(
                anyhow!("SnowflakeSink only supports append-only mode at present, please change the query to append-only, or use `force_append_only = 'true'`")
            ));
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for SnowflakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SnowflakeConfig::from_hashmap(param.properties)?;
        Ok(SnowflakeSink {
            config,
            schema,
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

pub struct SnowflakeSinkWriter {
    config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    /// the client used to send `insertFiles` post request
    http_client: SnowflakeHttpClient,
    /// the client to insert file to external storage (i.e., s3)
    s3_client: SnowflakeS3Client,
    row_encoder: JsonEncoder,
    /// The current epoch, used in naming the sink files
    /// mainly used for debugging purpose
    epoch: u64,
    /// streaming uploader, i.e., opendal s3 engine
    streaming_uploader: Option<Box<dyn StreamingUploader>>,
    /// the *unique* file suffix for intermediate s3 files
    file_suffix: Option<String>,
    /// the flag that indicates whether we have at least call `streaming_upload`
    /// once during this epoch, this is used to prevent uploading empty data.
    has_data: bool,
}

impl SnowflakeSinkWriter {
    pub fn new(
        config: SnowflakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let http_client = SnowflakeHttpClient::new(
            config.common.account_identifier.clone(),
            config.common.user.clone(),
            config.common.database.clone(),
            config.common.schema.clone(),
            config.common.pipe.clone(),
            config.common.rsa_public_key_fp.clone(),
            config.common.private_key.clone(),
            HashMap::new(),
            config.common.s3_path.clone(),
        );

        let s3_client = SnowflakeS3Client::new(
            config.common.s3_bucket.clone(),
            config.common.s3_path.clone(),
            config.common.aws_access_key_id.clone(),
            config.common.aws_secret_access_key.clone(),
            config.common.aws_region.clone(),
        )?;

        Ok(Self {
            config,
            schema: schema.clone(),
            pk_indices,
            is_append_only,
            http_client,
            s3_client,
            row_encoder: JsonEncoder::new(
                schema,
                None,
                super::encoder::DateHandlingMode::String,
                TimestampHandlingMode::String,
                TimestamptzHandlingMode::UtcString,
                TimeHandlingMode::String,
            ),
            // initial value of `epoch` will start from 0
            epoch: 0,
            // will be initialized after the begin of each epoch
            streaming_uploader: None,
            file_suffix: None,
            has_data: false,
        })
    }

    /// update the streaming uploader as well as the file suffix.
    /// note: should *only* be called when a new epoch begins.
    async fn update_streaming_uploader(&mut self) -> Result<()> {
        self.file_suffix = Some(self.file_suffix());
        let path = generate_s3_file_name(self.s3_client.s3_path(), self.file_suffix.as_ref().unwrap());
        let uploader = self
            .s3_client
            .opendal_s3_engine
            .streaming_upload(&path)
            .await
            .map_err(|err| {
                SinkError::Snowflake(format!(
                    "failed to create the streaming uploader of opendal s3 engine for epoch {}, error: {}",
                    self.epoch,
                    err
                ))
            })?;
        self.streaming_uploader = Some(uploader);
        // we don't have data at the beginning of each epoch
        self.has_data = false;
        Ok(())
    }

    /// write data to the current streaming uploader for this epoch.
    async fn streaming_upload(&mut self, data: Bytes) -> Result<()> {
        debug_assert!(self.streaming_uploader.is_some(), "expect streaming uploader to be properly initialized");
        self
            .streaming_uploader
            .as_mut()
            .unwrap()
            .write_bytes(data)
            .await
            .map_err(|err| {
                SinkError::Snowflake(format!(
                    "failed to write bytes when streaming uploading to s3 for snowflake sink, error: {}",
                    err
                ))
            })?;
        // well, at least there are some data to be uploaded
        self.has_data = true;
        Ok(())
    }

    /// finalize streaming upload for this epoch.
    /// ensure all the data has been properly uploaded to intermediate s3.
    async fn finish_streaming_upload(&mut self) -> Result<()> {
        let uploader = std::mem::take(&mut self.streaming_uploader);
        let Some(uploader) = uploader else {
            return Err(SinkError::Snowflake(format!(
                "streaming uploader is not valid when trying to finish streaming upload for epoch {}",
                self.epoch
                )
            ));
        };
        uploader.finish().await.map_err(|err| {
            SinkError::Snowflake(format!(
                "failed to finish streaming upload to s3 for snowflake sink, error: {}",
                err
            ))
        })?;
        Ok(())
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert, "expect all `op(s)` to be `Op::Insert`");
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.streaming_upload(row_json_string.into()).await?;
        }
        Ok(())
    }

    fn update_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// generate a *global unique* uuid,
    /// which is the key to the uniqueness of file suffix.
    fn gen_uuid() -> Uuid {
        Uuid::new_v4()
    }

    /// construct the *global unique* file suffix for the sink.
    /// note: this is unique even across multiple parallel writer(s).
    fn file_suffix(&self) -> String {
        // the format of suffix will be <epoch>_<uuid>
        format!("{}_{}", self.epoch, Self::gen_uuid())
    }

    /// sink `payload` to s3, then trigger corresponding `insertFiles` post request
    /// to snowflake, to finish the overall sinking pipeline.
    async fn commit(&mut self) -> Result<()> {
        if !self.has_data {
            // no data needs to be committed
            return Ok(());
        }
        self.finish_streaming_upload().await?;
        // trigger `insertFiles` post request to snowflake
        self.http_client.send_request(self.file_suffix.as_ref().unwrap().as_str()).await?;
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.update_epoch(epoch);
        self.update_streaming_uploader().await?;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            // finalize current streaming upload, plus notify snowflake to sink
            // the corresponding data to snowflake pipe.
            self.commit().await?;
        }
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.append_only(chunk).await?;
        Ok(())
    }
}
