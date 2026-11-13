// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use fail::fail_point;
use futures::StreamExt;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::raw::BoxedStaticFuture;
use opendal::services::Memory;
use opendal::{Execute, Executor, Operator, Writer};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::range::RangeBoundsExt;
use thiserror_ext::AsReport;

use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::{
    MonitoredObjectStore, ObjectDataStream, ObjectError, ObjectMetadata, ObjectMetadataIter,
    ObjectRangeBounds, ObjectResult, ObjectStore, OperationType, StreamingUploader, prefix,
};

/// Opendal object storage.
#[derive(Clone)]
pub struct OpendalObjectStore {
    pub(crate) op: Operator,
    pub(crate) media_type: MediaType,
    pub(crate) config: Arc<ObjectStoreConfig>,
    pub(crate) metrics: Arc<ObjectStoreMetrics>,
}

#[derive(Clone)]
pub enum MediaType {
    Memory,
    Hdfs,
    Gcs,
    Minio,
    S3,
    Obs,
    Oss,
    Webhdfs,
    Azblob,
    Fs,
}

impl MediaType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MediaType::Memory => "Memory",
            MediaType::Hdfs => "Hdfs",
            MediaType::Gcs => "Gcs",
            MediaType::Minio => "Minio",
            MediaType::S3 => "S3",
            MediaType::Obs => "Obs",
            MediaType::Oss => "Oss",
            MediaType::Webhdfs => "Webhdfs",
            MediaType::Azblob => "Azblob",
            MediaType::Fs => "Fs",
        }
    }
}

impl OpendalObjectStore {
    /// create opendal memory engine, used for unit tests.
    pub fn test_new_memory_engine() -> ObjectResult<Self> {
        // Create memory backend builder.
        let builder = Memory::default();
        let op: Operator = Operator::new(builder)?;

        Ok(Self {
            op,
            media_type: MediaType::Memory,
            config: Arc::new(ObjectStoreConfig::default()),
            metrics: Arc::new(ObjectStoreMetrics::unused()),
        })
    }
}

#[inline]
fn timestamp_to_secs(t: opendal::raw::Timestamp) -> f64 {
    t.into_inner().as_second() as f64
}

#[async_trait::async_trait]
impl ObjectStore for OpendalObjectStore {
    type StreamingUploader = OpendalStreamingUploader;

    fn get_object_prefix(&self, obj_id: u64, use_new_object_prefix_strategy: bool) -> String {
        match self.media_type {
            MediaType::S3 => prefix::s3::get_object_prefix(obj_id),
            MediaType::Minio => prefix::s3::get_object_prefix(obj_id),
            MediaType::Memory => String::default(),
            MediaType::Hdfs
            | MediaType::Gcs
            | MediaType::Obs
            | MediaType::Oss
            | MediaType::Webhdfs
            | MediaType::Azblob
            | MediaType::Fs => {
                prefix::opendal_engine::get_object_prefix(obj_id, use_new_object_prefix_strategy)
            }
        }
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            self.op.write(path, obj).await?;
            Ok(())
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<Self::StreamingUploader> {
        if self.op.info().capability().write_can_multi {
            Ok(OpendalStreamingUploader::Native(Box::new(
                OpendalNativeStreamingUploader::new(
                    self.op.clone(),
                    path.to_owned(),
                    self.config.clone(),
                    self.metrics.clone(),
                    self.store_media_type(),
                    false,
                )
                .await?,
            )))
        } else {
            Ok(OpendalStreamingUploader::Buffered(
                OpendalBufferedStreamingUploader::new(
                    self.clone()
                        .monitored(self.metrics.clone(), self.config.clone()),
                    path.to_owned(),
                ),
            ))
        }
    }

    async fn streaming_upload_owned(&self, path: &str) -> ObjectResult<Self::StreamingUploader> {
        if matches!(self.media_type, MediaType::Fs) {
            Ok(OpendalStreamingUploader::Native(Box::new(
                OpendalNativeStreamingUploader::new(
                    self.op.clone(),
                    path.to_owned(),
                    self.config.clone(),
                    self.metrics.clone(),
                    self.store_media_type(),
                    true,
                )
                .await?,
            )))
        } else {
            self.streaming_upload(path).await
        }
    }

    async fn available_space(&self) -> ObjectResult<Option<u64>> {
        if !matches!(self.media_type, MediaType::Fs) {
            return Ok(None);
        }
        let root = self.op.info().root();
        tokio::task::spawn_blocking(move || fs4::available_space(root))
            .await
            .map_err(|error| {
                ObjectError::internal(format!(
                    "filesystem space query failed: {}",
                    error.as_report()
                ))
            })?
            .map(Some)
            .map_err(|error| {
                ObjectError::internal(format!(
                    "filesystem space query failed: {}",
                    error.as_report()
                ))
            })
    }

    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let data = if range.is_full() {
            self.op.read(path).await?
        } else {
            self.op
                .read_with(path)
                .range(range.map(|v| *v as u64))
                .await?
        };

        if let Some(len) = range.len()
            && len != data.len()
        {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                len,
                data.len(),
                path,
                range,
            )));
        }

        Ok(data.to_bytes())
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        range: impl ObjectRangeBounds,
    ) -> ObjectResult<ObjectDataStream> {
        fail_point!("opendal_streaming_read_err", |_| Err(
            ObjectError::internal("opendal streaming read error")
        ));
        let range = range.map(|v| *v as u64);

        // The layer specified first will be executed first.
        // `TimeoutLayer` must be specified before `RetryLayer`.
        // Otherwise, it will lead to bad state inside OpenDAL and panic.
        // See https://docs.rs/opendal/latest/opendal/layers/struct.RetryLayer.html#panics
        let op = self
            .op
            .clone()
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(
                self.config.retry.streaming_read_attempt_timeout_ms,
            )))
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(
                        self.config.retry.req_backoff_interval_ms,
                    ))
                    .with_max_delay(Duration::from_millis(
                        self.config.retry.req_backoff_max_delay_ms,
                    ))
                    .with_max_times(self.config.retry.streaming_read_retry_attempts)
                    .with_factor(self.config.retry.req_backoff_factor as f32)
                    .with_jitter(),
            );
        let mut reader = op.reader_with(path);
        if self.config.opendal_streaming_read_chunk_size > 0 {
            // OpenDAL fully buffers each range before yielding, with a separate limit on the
            // number of range requests in flight.
            reader = reader
                .chunk(self.config.opendal_streaming_read_chunk_size)
                .concurrent(self.config.opendal_streaming_read_chunk_concurrency);
        }
        let stream = reader.await?.into_bytes_stream(range).await?.map(|item| {
            item.map(|b| Bytes::copy_from_slice(b.as_ref()))
                .map_err(|e| {
                    ObjectError::internal(format!("reader into_stream fail {}", e.as_report()))
                })
        });

        Ok(Box::pin(stream))
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let opendal_metadata = self.op.stat(path).await?;
        let key = path.to_owned();
        let last_modified = match opendal_metadata.last_modified() {
            Some(t) => timestamp_to_secs(t),
            None => 0_f64,
        };

        let total_size = opendal_metadata.content_length() as usize;
        let metadata = ObjectMetadata {
            key,
            last_modified,
            total_size,
        };
        Ok(metadata)
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        self.op.delete(path).await?;
        Ok(())
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        self.op.delete_iter(paths.to_vec()).await?;
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> ObjectResult<ObjectMetadataIter> {
        let mut object_lister = self.op.lister_with(prefix).recursive(true);
        if let Some(start_after) = start_after {
            object_lister = object_lister.start_after(&start_after);
        }
        let object_lister = object_lister.await?;

        let op = self.op.clone();
        let stream = object_lister.then(move |object| {
            let op = op.clone();
            async move { Self::listed_object_metadata(&op, object?).await }
        });

        Ok(stream.take(limit.unwrap_or(usize::MAX)).boxed())
    }

    fn store_media_type(&self) -> &'static str {
        self.media_type.as_str()
    }
}

impl OpendalObjectStore {
    async fn listed_object_metadata(
        op: &Operator,
        object: opendal::Entry,
    ) -> ObjectResult<ObjectMetadata> {
        let key = object.path().to_owned();
        // OpenDAL 0.55 removed list metadata capability flags and reports unknown sizes as 0.
        let meta = object.metadata();
        let mut last_modified = meta.last_modified().map(timestamp_to_secs);
        let mut total_size = meta.content_length() as usize;
        // FS listing includes the actual size, even for empty files. Avoid a redundant stat
        // that can fail if an atomic upload renames the listed temporary file.
        if last_modified.is_none()
            || (total_size == 0 && op.info().scheme() != opendal::services::FS_SCHEME)
        {
            // Propagate stat failures; treating one as EOF can make recovery trust a partial scan.
            let stat_meta = op.stat(&key).await?;
            last_modified = stat_meta.last_modified().map(timestamp_to_secs);
            total_size = stat_meta.content_length() as usize;
        }
        Ok(ObjectMetadata {
            key,
            last_modified: last_modified.unwrap_or(0_f64),
            total_size,
        })
    }

    pub async fn copy(&self, from_path: &str, to_path: &str) -> ObjectResult<()> {
        self.op.copy(from_path, to_path).await?;
        Ok(())
    }
}

struct OpendalStreamingUploaderExecute {
    /// To record metrics for uploading part.
    metrics: Arc<ObjectStoreMetrics>,
    media_type: &'static str,
}

impl OpendalStreamingUploaderExecute {
    const STREAMING_UPLOAD_TYPE: OperationType = OperationType::StreamingUpload;

    fn new(metrics: Arc<ObjectStoreMetrics>, media_type: &'static str) -> Self {
        Self {
            metrics,
            media_type,
        }
    }
}

impl Execute for OpendalStreamingUploaderExecute {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        let operation_type_str = Self::STREAMING_UPLOAD_TYPE.as_str();
        let media_type = self.media_type;

        let metrics = self.metrics.clone();
        let _handle = tokio::spawn(async move {
            let _timer = metrics
                .operation_latency
                .with_label_values(&[media_type, operation_type_str])
                .start_timer();

            f.await
        });
    }
}

pub enum OpendalStreamingUploader {
    Native(Box<OpendalNativeStreamingUploader>),
    Buffered(OpendalBufferedStreamingUploader),
}

/// Store multiple parts in a map, and concatenate them on finish.
pub struct OpendalNativeStreamingUploader {
    writer: Writer,
    /// Buffer for data. It will store at least `UPLOAD_BUFFER_SIZE` bytes of data before wrapping itself
    /// into a stream and upload to object store as a part.
    buf: Vec<Bytes>,
    /// Length of the data that have not been uploaded to object store.
    not_uploaded_len: usize,
    /// Whether the writer is valid. The writer is invalid after abort/close.
    is_valid: bool,

    abort_on_err: bool,

    upload_part_size: usize,
}

impl OpendalNativeStreamingUploader {
    async fn new(
        op: Operator,
        path: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
        media_type: &'static str,
        owned_fs: bool,
    ) -> ObjectResult<Self> {
        let monitored_execute = OpendalStreamingUploaderExecute::new(metrics, media_type);
        let executor = Executor::with(monitored_execute);
        // Start from the base context so existing layers are replayed exactly once by
        // `with_context`. Using the composed context here would double-wrap resources such as
        // the HTTP concurrency semaphore.
        let ctx = op.base_context().with_executor(executor);
        let op = op.with_context(ctx);

        let writer = if owned_fs {
            // The caller owns a task that is not dropped on waiter cancellation. No
            // timeout may abandon a blocking FS operation before known cleanup is queued.
            op.writer_with(&path).concurrent(1).await?
        } else {
            op.clone()
                .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(
                    config.retry.streaming_upload_attempt_timeout_ms,
                )))
                .layer(
                    RetryLayer::new()
                        .with_min_delay(Duration::from_millis(config.retry.req_backoff_interval_ms))
                        .with_max_delay(Duration::from_millis(
                            config.retry.req_backoff_max_delay_ms,
                        ))
                        .with_max_times(config.retry.streaming_upload_retry_attempts)
                        .with_factor(config.retry.req_backoff_factor as f32)
                        .with_jitter(),
                )
                .writer_with(&path)
                .concurrent(config.opendal_upload_concurrency)
                .await?
        };
        Ok(Self {
            writer,
            buf: vec![],
            not_uploaded_len: 0,
            is_valid: true,
            abort_on_err: owned_fs || config.opendal_writer_abort_on_err,
            upload_part_size: config.upload_part_size,
        })
    }

    async fn flush(&mut self) -> ObjectResult<()> {
        let data = std::mem::take(&mut self.buf);
        debug_assert_eq!(
            data.iter().map(|b| b.len()).sum::<usize>(),
            self.not_uploaded_len
        );
        if let Err(err) = self.writer.write(data).await {
            self.is_valid = false;
            if self.abort_on_err
                && let Err(cleanup_error) = self.writer.abort().await
            {
                tracing::warn!(error = %cleanup_error.as_report(), "failed to abort errored upload; retaining original upload error");
            }
            return Err(err.into());
        }
        self.not_uploaded_len = 0;
        Ok(())
    }
}

impl StreamingUploader for OpendalNativeStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        assert!(self.is_valid);
        self.not_uploaded_len += data.len();
        self.buf.push(data);
        if self.not_uploaded_len >= self.upload_part_size {
            self.flush().await?;
        }
        Ok(())
    }

    async fn finish(mut self) -> ObjectResult<()> {
        assert!(self.is_valid);
        if self.not_uploaded_len > 0 {
            self.flush().await?;
        }

        assert!(self.buf.is_empty());
        assert_eq!(self.not_uploaded_len, 0);

        self.is_valid = false;
        match self.writer.close().await {
            Ok(_) => (),
            Err(err) => {
                if self.abort_on_err
                    && let Err(cleanup_error) = self.writer.abort().await
                {
                    tracing::warn!(error = %cleanup_error.as_report(), "failed to abort errored upload; retaining original upload error");
                }
                return Err(err.into());
            }
        };

        Ok(())
    }

    // Not absolutely accurate. Some bytes may be in the infight request.
    fn get_memory_usage(&self) -> u64 {
        self.not_uploaded_len as u64
    }
}

pub struct OpendalBufferedStreamingUploader {
    store: MonitoredObjectStore<OpendalObjectStore>,
    path: String,
    buf: BytesMut,
}

impl OpendalBufferedStreamingUploader {
    fn new(store: MonitoredObjectStore<OpendalObjectStore>, path: String) -> Self {
        Self {
            store,
            path,
            buf: BytesMut::new(),
        }
    }
}

impl StreamingUploader for OpendalBufferedStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        self.buf.extend_from_slice(&data);
        Ok(())
    }

    async fn finish(self) -> ObjectResult<()> {
        self.store.upload(&self.path, self.buf.freeze()).await
    }

    fn get_memory_usage(&self) -> u64 {
        self.buf.len() as u64
    }
}

impl StreamingUploader for OpendalStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        match self {
            Self::Native(uploader) => uploader.write_bytes(data).await,
            Self::Buffered(uploader) => uploader.write_bytes(data).await,
        }
    }

    async fn finish(self) -> ObjectResult<()> {
        match self {
            Self::Native(uploader) => (*uploader).finish().await,
            Self::Buffered(uploader) => uploader.finish().await,
        }
    }

    fn get_memory_usage(&self) -> u64 {
        match self {
            Self::Native(uploader) => uploader.get_memory_usage(),
            Self::Buffered(uploader) => uploader.get_memory_usage(),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::*;
    use crate::object::{InMemObjectStore, ObjectStoreImpl};

    #[cfg(not(madsim))] // OpenDAL's HTTP client uses real sockets.
    mod http_streaming_read_tests {
        use std::sync::Mutex;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio::sync::Notify;
        use tokio::task::{JoinHandle, JoinSet};

        use super::*;
        use crate::object::ObjectDataStreamReader;

        #[derive(Clone, Copy)]
        enum FirstRequest {
            Respond,
            Stall,
            WaitForConcurrent,
        }

        /// Serve a fixed object over the S3 range-GET protocol, recording physical HTTP requests.
        /// Control the first response to exercise the read deadline and concurrent requests.
        async fn mock_s3_streaming_reader(
            chunk_size: usize,
            chunk_concurrency: usize,
            first_request: FirstRequest,
        ) -> (ObjectStoreImpl, Arc<Mutex<Vec<String>>>, JoinHandle<()>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = requests.clone();
            let server = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                let next_request = Arc::new(Notify::new());
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let recorded = recorded.clone();
                    let next_request = next_request.clone();
                    connections.spawn(async move {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut buf = [0; 1024];
                        let n = socket.read(&mut buf).await.unwrap();
                        assert_ne!(n, 0);
                        request.extend_from_slice(&buf[..n]);
                    }
                    let request = String::from_utf8(request).unwrap();
                    let range = request
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("range").then_some(value.trim())
                        })
                        .unwrap();
                    let first = {
                        let mut requests = recorded.lock().unwrap();
                        requests.push(range.to_owned());
                        requests.len() == 1
                    };
                    if first {
                        match first_request {
                            FirstRequest::Respond => {}
                            FirstRequest::Stall => std::future::pending::<()>().await,
                            FirstRequest::WaitForConcurrent => next_request.notified().await,
                        }
                    } else {
                        next_request.notify_one();
                    }
                    let (start, end) = range.strip_prefix("bytes=").unwrap().split_once('-').unwrap();
                    let start: usize = start.parse().unwrap();
                    let end: usize = end.parse().unwrap();
                    let body = &b"0123456789abcdef"[start..=end];
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/16\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                });
                    while connections.try_join_next().is_some() {}
                }
            });
            let mut config = ObjectStoreConfig {
                opendal_streaming_read_chunk_size: chunk_size,
                opendal_streaming_read_chunk_concurrency: chunk_concurrency,
                ..Default::default()
            };
            config.retry.streaming_read_attempt_timeout_ms = match first_request {
                FirstRequest::Respond => 10_000,
                _ => 1000,
            };
            config.retry.streaming_read_retry_attempts = 1;
            config.retry.req_backoff_interval_ms = 1;
            config.retry.req_backoff_max_delay_ms = 1;
            let config = Arc::new(config);
            let metrics = Arc::new(ObjectStoreMetrics::unused());
            let store = OpendalObjectStore::new_minio_engine(
                &format!("minio://key:secret@{addr}/bucket"),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
            (
                ObjectStoreImpl::Opendal(store.monitored(metrics, config)),
                requests,
                server,
            )
        }

        #[tokio::test]
        async fn test_opendal_chunked_streaming_read_requests() {
            for (chunk_size, chunk_concurrency) in [(0, 1), (4, 0), (4, 1)] {
                let (store, requests, server) =
                    mock_s3_streaming_reader(chunk_size, chunk_concurrency, FirstRequest::Respond)
                        .await;
                let reader = store.streaming_read("data", 2..13).await.unwrap();
                assert_eq!(reader.streaming_read_timeout, Some(Duration::from_secs(10)));
                assert!(requests.lock().unwrap().is_empty());
                let mut reader = ObjectDataStreamReader::new(reader.into_stream());
                let mut buf = [0; 4];
                reader.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"2345");
                assert_eq!(requests.lock().unwrap().len(), 1);
                reader.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"6789");
                let mut tail = Vec::new();
                reader.read_to_end(&mut tail).await.unwrap();
                assert_eq!(tail, b"abc");
                let expected = if chunk_size > 0 {
                    vec!["bytes=2-5", "bytes=6-9", "bytes=10-12"]
                } else {
                    vec!["bytes=2-12"]
                };
                assert_eq!(*requests.lock().unwrap(), expected);
                server.abort();
                let _ = server.await;
            }
        }

        #[tokio::test]
        async fn test_opendal_chunked_streaming_read_timeout() {
            let (store, requests, server) =
                mock_s3_streaming_reader(4, 1, FirstRequest::Stall).await;
            let ObjectStoreImpl::Opendal(store) = store else {
                unreachable!()
            };
            // Construct the monitored reader directly, without the enum dispatch wrapper.
            let mut reader = store.streaming_read("data", 2..6).await.unwrap();
            assert_eq!(reader.streaming_read_timeout, Some(Duration::from_secs(1)));
            // Reuse the existing deadline for the entire wait, including backend attempts/backoff.
            let err = reader.read_bytes().await.unwrap().unwrap_err();
            assert!(matches!(
                err.inner(),
                crate::object::ObjectErrorInner::Timeout(_)
            ));
            assert_eq!(*requests.lock().unwrap(), ["bytes=2-5"]);
            server.abort();
            let _ = server.await;
        }

        #[tokio::test]
        async fn test_opendal_chunked_streaming_read_concurrency() {
            let (store, requests, server) =
                mock_s3_streaming_reader(4, 2, FirstRequest::WaitForConcurrent).await;
            let reader = store.streaming_read("data", 2..10).await.unwrap();
            let mut reader = ObjectDataStreamReader::new(reader.into_stream());
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"23456789");
            {
                let requests = requests.lock().unwrap();
                assert_eq!(requests.len(), 2);
                // The first response cannot complete until the other range starts. Sequential
                // reads would hit the read deadline instead of reaching this assertion.
                assert_ne!(requests[0], requests[1]);
            }
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn test_opendal_chunk_config_does_not_change_other_backends() {
        let config = Arc::new(ObjectStoreConfig {
            opendal_streaming_read_chunk_size: 4,
            ..Default::default()
        });
        let timeout = Duration::from_millis(config.retry.streaming_read_attempt_timeout_ms);
        let store = ObjectStoreImpl::InMem(
            InMemObjectStore::default().monitored(Arc::new(ObjectStoreMetrics::unused()), config),
        );
        store
            .upload("data", Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        let mut reader = store.streaming_read("data", 2..9).await.unwrap();
        assert_eq!(reader.streaming_read_timeout, Some(timeout));
        assert_eq!(reader.read_bytes().await.unwrap().unwrap(), "2345678");
        assert!(reader.read_bytes().await.is_none());
    }

    async fn list_all(prefix: &str, store: &OpendalObjectStore) -> Vec<ObjectMetadata> {
        store
            .list(prefix, None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_memory_upload() {
        let block = Bytes::from("123456");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("/abc", block).await.unwrap();

        // No such object.
        store.read("/ab", 0..3).await.unwrap_err();

        let bytes = store.read("/abc", 4..6).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_owned());

        store.delete("/abc").await.unwrap();

        // No such object.
        store.read("/abc", 0..3).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_memory_read_out_of_range_returns_error() {
        let block = Bytes::from("123456");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("/abc", block).await.unwrap();

        // OpenDAL 0.58 reports an out-of-range read instead of panicking.
        store.read("/abc", 4..44).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_memory_metadata() {
        let block = Bytes::from("123456");
        let path = "/abc".to_owned();
        let obj_store = OpendalObjectStore::test_new_memory_engine().unwrap();
        obj_store.upload("/abc", block).await.unwrap();

        let err = obj_store.metadata("/not_exist").await.unwrap_err();
        assert!(err.is_object_not_found_error());
        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
        obj_store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_buffered_streaming_upload() {
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        let mut uploader = OpendalBufferedStreamingUploader::new(
            store
                .clone()
                .monitored(store.metrics.clone(), store.config.clone()),
            "/abc".to_owned(),
        );

        uploader.write_bytes(Bytes::from("123")).await.unwrap();
        assert_eq!(uploader.get_memory_usage(), 3);
        uploader.write_bytes(Bytes::from("456")).await.unwrap();
        assert_eq!(uploader.get_memory_usage(), 6);
        uploader.finish().await.unwrap();

        let read_obj = store.read("/abc", ..).await.unwrap();
        assert_eq!(read_obj, Bytes::from("123456"));
    }

    #[tokio::test]
    async fn test_empty_buffered_streaming_upload() {
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        let uploader = OpendalBufferedStreamingUploader::new(
            store
                .clone()
                .monitored(store.metrics.clone(), store.config.clone()),
            "/abc".to_owned(),
        );

        uploader.finish().await.unwrap_err();
    }

    #[cfg(not(madsim))] // OpenDAL FS uses real filesystem I/O.
    #[tokio::test]
    async fn test_listed_empty_fs_file_survives_rename() {
        let dir = tempfile::tempdir().unwrap();
        let op = Operator::new(opendal::services::Fs::default().root(dir.path().to_str().unwrap()))
            .unwrap();
        tokio::fs::write(dir.path().join("sst.tmp"), b"")
            .await
            .unwrap();
        let mut objects = op.lister_with("").recursive(true).await.unwrap();
        let object = objects.next().await.unwrap().unwrap();
        assert_eq!(object.path(), "sst.tmp");
        assert_eq!(object.metadata().content_length(), 0);
        let last_modified = timestamp_to_secs(object.metadata().last_modified().unwrap());

        // Model an atomic upload finishing after LIST has captured the temporary file.
        tokio::fs::rename(dir.path().join("sst.tmp"), dir.path().join("sst"))
            .await
            .unwrap();
        let metadata = OpendalObjectStore::listed_object_metadata(&op, object)
            .await
            .unwrap();
        assert_eq!(metadata.key, "sst.tmp");
        assert_eq!(metadata.total_size, 0);
        assert_eq!(metadata.last_modified, last_modified);
    }

    #[tokio::test]
    async fn test_listed_object_metadata_propagates_stat_failure() {
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store
            .upload("sst", Bytes::from_static(b"data"))
            .await
            .unwrap();
        // Capture the listed entry before deletion so the subsequent metadata lookup fails.
        // Memory entries have no last_modified, so our adapter must stat even a listed file.
        let mut objects = store.op.lister_with("").recursive(true).await.unwrap();
        let object = objects.next().await.unwrap().unwrap();
        store.delete("sst").await.unwrap();
        let error = OpendalObjectStore::listed_object_metadata(&store.op, object)
            .await
            .unwrap_err();
        assert!(error.is_object_not_found_error());
    }

    #[tokio::test]
    async fn test_memory_delete_objects_and_list_object() {
        let block1 = Bytes::from("123456");
        let block2 = Bytes::from("987654");
        let store = OpendalObjectStore::test_new_memory_engine().unwrap();
        store.upload("abc", Bytes::from("123456")).await.unwrap();
        store.upload("prefix/abc", block1).await.unwrap();

        store.upload("prefix/xyz", block2).await.unwrap();

        assert_eq!(list_all("", &store).await.len(), 3);
        assert_eq!(list_all("prefix/", &store).await.len(), 2);
        let str_list = [String::from("prefix/abc"), String::from("prefix/xyz")];

        store.delete_objects(&str_list).await.unwrap();

        assert!(store.read("prefix/abc/", ..).await.is_err());
        assert!(store.read("prefix/xyz/", ..).await.is_err());
        assert_eq!(list_all("", &store).await.len(), 1);
        assert_eq!(list_all("prefix/", &store).await.len(), 0);
    }
}
