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

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_object_store::object::object_metrics::GLOBAL_OBJECT_STORE_METRICS;
use risingwave_object_store::object::{
    ObjectStore, OpendalObjectStore, OpendalStreamingUploader, StreamingUploader,
};
use serde::Deserialize;
use serde_json::Value;
use serde_with::serde_as;
use uuid::Uuid;
use with_options::WithOptions;

use super::encoder::{
    JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use super::writer::LogSinkerOf;
use super::{SinkError, SinkParam, SinkWriterMetrics};
use crate::sink::writer::SinkWriterExt;
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

pub const SNOWFLAKE_SINK: &str = "snowflake";
const S3_INTERMEDIATE_FILE_NAME: &str = "RW_SNOWFLAKE_S3_SINK_FILE";

#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct SnowflakeCommon {
    /// The s3 bucket where intermediate sink files will be stored
    #[serde(rename = "snowflake.s3_bucket", alias = "s3.bucket_name")]
    pub s3_bucket: String,

    /// The optional s3 path to be specified
    /// the actual file location would be `s3://<s3_bucket>/<s3_path>/<rw_auto_gen_intermediate_file_name>`
    /// if this field is specified by user(s)
    /// otherwise it would be `s3://<s3_bucket>/<rw_auto_gen_intermediate_file_name>`
    #[serde(rename = "snowflake.s3_path", alias = "s3.path")]
    pub s3_path: Option<String>,

    /// s3 credentials
    #[serde(
        rename = "snowflake.aws_access_key_id",
        alias = "s3.credentials.access"
    )]
    pub aws_access_key_id: String,

    /// s3 credentials
    #[serde(
        rename = "snowflake.aws_secret_access_key",
        alias = "s3.credentials.secret"
    )]
    pub aws_secret_access_key: String,

    /// The s3 region, e.g., us-east-2
    #[serde(rename = "snowflake.aws_region", alias = "s3.region_name")]
    pub aws_region: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(flatten)]
    pub common: SnowflakeCommon,
}

impl SnowflakeConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
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
        .into_log_sinker(SinkWriterMetrics::new(&writer_param)))
    }

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::SnowflakeSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
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
        let config = SnowflakeConfig::from_btreemap(param.properties)?;
        Ok(SnowflakeSink {
            config,
            schema,
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

pub struct SnowflakeSinkWriter {
    #[expect(dead_code)]
    config: SnowflakeConfig,
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    #[expect(dead_code)]
    is_append_only: bool,
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
    streaming_uploader: Option<OpendalStreamingUploader>,
}

impl SnowflakeSinkWriter {
    pub fn new(
        config: SnowflakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
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
            s3_client,
            row_encoder: JsonEncoder::new(
                schema,
                None,
                super::encoder::DateHandlingMode::String,
                TimestampHandlingMode::String,
                TimestamptzHandlingMode::UtcString,
                TimeHandlingMode::String,
                JsonbHandlingMode::String,
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
    async fn new_streaming_uploader(&mut self) -> Result<OpendalStreamingUploader> {
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
        Ok(uploader)
    }

    /// write data to the current streaming uploader for this epoch.
    async fn streaming_upload(&mut self, data: Bytes) -> Result<()> {
        let uploader = match self.streaming_uploader.as_mut() {
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
    async fn finish_streaming_upload(&mut self) -> Result<()> {
        let uploader = std::mem::take(&mut self.streaming_uploader);
        let Some(uploader) = uploader else {
            // there is no data to be uploaded for this epoch
            return Ok(());
        };
        uploader
            .finish()
            .await
            .context("failed to finish streaming upload to s3")
            .map_err(SinkError::Snowflake)?;
        Ok(())
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
        self.finish_streaming_upload().await
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

/// The helper function to generate the *global unique* s3 file name.
pub(crate) fn generate_s3_file_name(s3_path: Option<&str>, suffix: &str) -> String {
    match s3_path {
        Some(path) => format!("{}/{}_{}", path, S3_INTERMEDIATE_FILE_NAME, suffix),
        None => format!("{}_{}", S3_INTERMEDIATE_FILE_NAME, suffix),
    }
}

/// todo: refactor this part after s3 sink is available
pub struct SnowflakeS3Client {
    #[expect(dead_code)]
    s3_bucket: String,
    s3_path: Option<String>,
    pub opendal_s3_engine: OpendalObjectStore,
}

impl SnowflakeS3Client {
    pub fn new(
        s3_bucket: String,
        s3_path: Option<String>,
        aws_access_key_id: String,
        aws_secret_access_key: String,
        aws_region: String,
    ) -> Result<Self> {
        // FIXME: we should use the `ObjectStoreConfig` instead of default
        // just use default configuration here for opendal s3 engine
        let config = ObjectStoreConfig::default();

        let metrics = Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone());

        // create the s3 engine for streaming upload to the intermediate s3 bucket
        let opendal_s3_engine = OpendalObjectStore::new_s3_engine_with_credentials(
            &s3_bucket,
            Arc::new(config),
            metrics,
            &aws_access_key_id,
            &aws_secret_access_key,
            &aws_region,
        )
        .context("failed to create opendal s3 engine")
        .map_err(SinkError::Snowflake)?;

        Ok(Self {
            s3_bucket,
            s3_path,
            opendal_s3_engine,
        })
    }

    pub fn s3_path(&self) -> Option<&str> {
        self.s3_path.as_deref()
    }
}
