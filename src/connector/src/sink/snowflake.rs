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
use std::fmt::Write;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_object_store::object::{ObjectStore, OpendalStreamingUploader, StreamingUploader};
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
const INITIAL_ROW_CAPACITY: usize = 1024;

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
    /// the actual file location would be `s3://<s3_bucket>/<s3_path>/<rw_auto_gen_intermediate_file_name>`
    /// if this field is specified by user(s)
    /// otherwise it would be `s3://<s3_bucket>/<rw_auto_gen_intermediate_file_name>`
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
    /// streaming uploader to upload data to the intermediate (s3) storage.
    /// this also contains the file suffix *unique* to the *local* sink writer per epoch.
    /// i.e., opendal s3 engine and the file suffix for intermediate s3 file.
    /// note: the option here *implicitly* indicates whether we have at
    /// least call `streaming_upload` once during this epoch,
    /// which is mainly used to prevent uploading empty data.
    streaming_uploader: Option<(OpendalStreamingUploader, String)>,
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
            // initial value of `epoch` will be set to 0
            epoch: 0,
            // will be (lazily) initialized after the begin of each epoch
            // when some data is ready to be upload
            streaming_uploader: None,
        })
    }

    /// return a brand new the streaming uploader as well as the file suffix.
    /// note: should *only* be called iff after a new epoch begins,
    /// and `streaming_upload` being called the first time.
    /// i.e., lazily initialization of the internal `streaming_uploader`.
    /// plus, this function is *pure*, the `&mut self` here is to make rustc (and tokio) happy.
    async fn new_streaming_uploader(&mut self) -> Result<(OpendalStreamingUploader, String)> {
        let file_suffix = self.file_suffix();
        let path = generate_s3_file_name(self.s3_client.s3_path(), &file_suffix);
        let uploader = self
            .s3_client
            .opendal_s3_engine
            .streaming_upload(&path)
            .await
            .with_context(|| {
                format!(
                    "failed to create the streaming uploader of opendal s3 engine for epoch {}",
                    self.epoch
                )
            })
            .map_err(SinkError::Snowflake)?;
        Ok((uploader, file_suffix))
    }

    /// write data to the current streaming uploader for this epoch.
    async fn streaming_upload(&mut self, data: Bytes) -> Result<()> {
        let (uploader, _) = match self.streaming_uploader.as_mut() {
            Some(s) => s,
            None => {
                assert!(
                    self.streaming_uploader.is_none(),
                    "expect `streaming_uploader` to be None"
                );
                let uploader = self.new_streaming_uploader().await?;
                self.streaming_uploader.insert(uploader)
            }
        };
        uploader
            .write_bytes(data)
            .await
            .context("failed to write bytes when streaming uploading to s3")
            .map_err(SinkError::Snowflake)?;
        Ok(())
    }

    /// finalize streaming upload for this epoch.
    /// ensure all the data has been properly uploaded to intermediate s3.
    async fn finish_streaming_upload(&mut self) -> Result<Option<String>> {
        let uploader = std::mem::take(&mut self.streaming_uploader);
        let Some((uploader, file_suffix)) = uploader else {
            // there is no data to be uploaded for this epoch
            return Ok(None);
        };
        uploader
            .finish()
            .await
            .context("failed to finish streaming upload to s3")
            .map_err(SinkError::Snowflake)?;
        Ok(Some(file_suffix))
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut chunk_buf = BytesMut::new();

        // write the json representations of the row(s) in current chunk to `chunk_buf`
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert, "expect all `op(s)` to be `Op::Insert`");
            // to prevent temporary string allocation,
            // so we directly write to `chunk_buf` implicitly via `write_fmt`.
            write!(
                chunk_buf,
                "{}",
                Value::Object(self.row_encoder.encode(row)?)
            )
            .unwrap(); // write to a `BytesMut` should never fail
        }

        // streaming upload in a chunk-by-chunk manner
        self.streaming_upload(chunk_buf.freeze()).await?;
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
        // note that after `finish_streaming_upload`, do *not* interact with
        // `streaming_uploader` until new data comes in at next epoch,
        // since the ownership has been taken in this method, and `None` will be left.
        let Some(file_suffix) = self.finish_streaming_upload().await? else {
            // represents there is no data to be uploaded for this epoch
            return Ok(());
        };
        // trigger `insertFiles` post request to snowflake
        self.http_client.send_request(&file_suffix).await?;
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.update_epoch(epoch);
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
            // note: if no data needs to be committed, then `commit` is simply a no-op.
            self.commit().await?;
        }
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.append_only(chunk).await?;
        Ok(())
    }
}
