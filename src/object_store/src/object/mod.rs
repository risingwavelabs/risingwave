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

#[cfg(madsim)]
pub mod sim;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

pub mod mem;
pub use mem::*;

pub mod opendal_engine;
pub use opendal_engine::*;

pub mod s3;
use await_tree::InstrumentAwait;
use futures::stream::BoxStream;
use futures::{Future, StreamExt};
pub use risingwave_common::config::ObjectStoreConfig;
pub use s3::*;

pub mod error;
pub mod object_metrics;

mod prefix;

pub use error::*;
use object_metrics::ObjectStoreMetrics;
use thiserror_ext::AsReport;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

#[cfg(madsim)]
use self::sim::SimObjectStore;

pub type ObjectStoreRef = Arc<ObjectStoreImpl>;
pub type ObjectStreamingUploader = MonitoredStreamingUploader;

type BoxedStreamingUploader = Box<dyn StreamingUploader>;

pub trait ObjectRangeBounds = RangeBounds<usize> + Clone + Send + Sync + std::fmt::Debug + 'static;

/// Partitions a set of given paths into two vectors. The first vector contains all local paths, and
/// the second contains all remote paths.
fn partition_object_store_paths(paths: &[String]) -> Vec<String> {
    // ToDo: Currently the result is a copy of the input. Would it be worth it to use an in-place
    //       partition instead?
    let mut vec_rem = vec![];

    for path in paths {
        vec_rem.push(path.to_string());
    }

    vec_rem
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMetadata {
    // Full path
    pub key: String,
    // Seconds since unix epoch.
    pub last_modified: f64,
    pub total_size: usize,
}

#[async_trait::async_trait]
pub trait StreamingUploader: Send {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()>;

    async fn finish(self: Box<Self>) -> ObjectResult<()>;

    fn get_memory_usage(&self) -> u64;
}

/// The implementation must be thread-safe.
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Get the key prefix for object
    fn get_object_prefix(&self, obj_id: u64) -> String;

    /// Uploads the object to `ObjectStore`.
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()>;

    async fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader>;

    /// If objects are PUT using a multipart upload, it's a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// <https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2>
    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes>;

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        read_range: Range<usize>,
    ) -> ObjectResult<ObjectDataStream>;

    /// Obtains the object metadata.
    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata>;

    /// Deletes blob permanently.
    async fn delete(&self, path: &str) -> ObjectResult<()>;

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()>;

    fn monitored(
        self,
        metrics: Arc<ObjectStoreMetrics>,
        config: Arc<ObjectStoreConfig>,
    ) -> MonitoredObjectStore<Self>
    where
        Self: Sized,
    {
        MonitoredObjectStore::new(self, metrics, config)
    }

    async fn list(&self, prefix: &str) -> ObjectResult<ObjectMetadataIter>;

    fn store_media_type(&self) -> &'static str;
}

pub enum ObjectStoreImpl {
    InMem(MonitoredObjectStore<InMemObjectStore>),
    Opendal(MonitoredObjectStore<OpendalObjectStore>),
    S3(MonitoredObjectStore<S3ObjectStore>),
    #[cfg(madsim)]
    Sim(MonitoredObjectStore<SimObjectStore>),
}

macro_rules! dispatch_async {
    ($object_store:expr, $method_name:ident $(, $args:expr)*) => {
        $object_store.$method_name($($args, )*).await
    }
}

/// This macro routes the object store operation to the real implementation by the `ObjectStoreImpl`
/// enum type and the `path`.
///
/// Except for `InMem`,the operation should be performed on remote object store.
macro_rules! object_store_impl_method_body {
    ($object_store:expr, $method_name:ident, $dispatch_macro:ident, $path:expr $(, $args:expr)*) => {
        {
            let path = $path;
            match $object_store {
                ObjectStoreImpl::InMem(in_mem) => {
                    $dispatch_macro!(in_mem, $method_name, path $(, $args)*)
                },
                ObjectStoreImpl::Opendal(opendal) => {
                    $dispatch_macro!(opendal, $method_name, path $(, $args)*)
                },
                ObjectStoreImpl::S3(s3) => {
                    $dispatch_macro!(s3, $method_name, path $(, $args)*)
                },
                #[cfg(madsim)]
                ObjectStoreImpl::Sim(in_mem) => {
                    $dispatch_macro!(in_mem, $method_name, path $(, $args)*)
                },
            }

        }
    };
}

/// This macro routes the object store operation to the real implementation by the `ObjectStoreImpl`
/// enum type and the `paths`. It is a modification of the macro above to work with a slice of
/// strings instead of just a single one.
///
/// Except for `InMem`, the operation should be performed on remote object store.
macro_rules! object_store_impl_method_body_slice {
    ($object_store:expr, $method_name:ident, $dispatch_macro:ident, $paths:expr $(, $args:expr)*) => {
        {
            let paths_rem = partition_object_store_paths($paths);

            match $object_store {
                ObjectStoreImpl::InMem(in_mem) => {
                    $dispatch_macro!(in_mem, $method_name, &paths_rem $(, $args)*)
                },
                ObjectStoreImpl::Opendal(opendal) => {
                    $dispatch_macro!(opendal, $method_name, &paths_rem $(, $args)*)
                },
                ObjectStoreImpl::S3(s3) => {
                    $dispatch_macro!(s3, $method_name, &paths_rem $(, $args)*)
                },
                #[cfg(madsim)]
                ObjectStoreImpl::Sim(in_mem) => {
                    $dispatch_macro!(in_mem, $method_name, &paths_rem $(, $args)*)
                },
            }
        }
    };
}

impl ObjectStoreImpl {
    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        object_store_impl_method_body!(self, upload, dispatch_async, path, obj)
    }

    pub async fn streaming_upload(&self, path: &str) -> ObjectResult<MonitoredStreamingUploader> {
        object_store_impl_method_body!(self, streaming_upload, dispatch_async, path)
    }

    pub async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        object_store_impl_method_body!(self, read, dispatch_async, path, range)
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        object_store_impl_method_body!(self, metadata, dispatch_async, path)
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    pub async fn streaming_read(
        &self,
        path: &str,
        start_loc: Range<usize>,
    ) -> ObjectResult<MonitoredStreamingReader> {
        object_store_impl_method_body!(self, streaming_read, dispatch_async, path, start_loc)
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        object_store_impl_method_body!(self, delete, dispatch_async, path)
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    ///
    /// If a hybrid storage is used, the method will first attempt to delete objects in local
    /// storage. Only if that is successful, it will remove objects from remote storage.
    pub async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        object_store_impl_method_body_slice!(self, delete_objects, dispatch_async, paths)
    }

    pub async fn list(&self, prefix: &str) -> ObjectResult<ObjectMetadataIter> {
        object_store_impl_method_body!(self, list, dispatch_async, prefix)
    }

    pub fn get_object_prefix(&self, obj_id: u64) -> String {
        // FIXME: ObjectStoreImpl lacks flexibility for adding new interface to ObjectStore
        // trait. Macro object_store_impl_method_body routes to local or remote only depending on
        // the path
        match self {
            ObjectStoreImpl::InMem(store) => store.inner.get_object_prefix(obj_id),
            ObjectStoreImpl::Opendal(store) => store.inner.get_object_prefix(obj_id),
            ObjectStoreImpl::S3(store) => store.inner.get_object_prefix(obj_id),
            #[cfg(madsim)]
            ObjectStoreImpl::Sim(store) => store.inner.get_object_prefix(obj_id),
        }
    }

    pub fn support_streaming_upload(&self) -> bool {
        match self {
            ObjectStoreImpl::InMem(_) => true,
            ObjectStoreImpl::Opendal(store) => {
                store.inner.op.info().native_capability().write_can_multi
            }
            ObjectStoreImpl::S3(_) => true,
            #[cfg(madsim)]
            ObjectStoreImpl::Sim(_) => true,
        }
    }
}

fn try_update_failure_metric<T>(
    metrics: &Arc<ObjectStoreMetrics>,
    result: &ObjectResult<T>,
    operation_type: &'static str,
) {
    if let Err(e) = &result {
        tracing::error!(error = %e.as_report(), "{} failed", operation_type);
        metrics
            .failure_count
            .with_label_values(&[operation_type])
            .inc();
    }
}

/// `MonitoredStreamingUploader` will report the following metrics.
/// - `write_bytes`: The number of bytes uploaded from the uploader's creation to finish.
/// - `operation_size`:
///   - `streaming_upload_write_bytes`: The number of bytes written for each call to `write_bytes`.
///   - `streaming_upload`: Same as `write_bytes`.
/// - `operation_latency`:
///   - `streaming_upload_start`: The time spent creating the uploader.
///   - `streaming_upload_write_bytes`: The time spent on each call to `write_bytes`.
///   - `streaming_upload_finish`: The time spent calling `finish`.
/// - `failure_count`: `streaming_upload_start`, `streaming_upload_write_bytes`,
///   `streaming_upload_finish`
pub struct MonitoredStreamingUploader {
    inner: BoxedStreamingUploader,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    /// Length of data uploaded with this uploader.
    operation_size: usize,
    media_type: &'static str,
}

impl MonitoredStreamingUploader {
    pub fn new(
        media_type: &'static str,
        handle: BoxedStreamingUploader,
        object_store_metrics: Arc<ObjectStoreMetrics>,
    ) -> Self {
        Self {
            inner: handle,
            object_store_metrics,
            operation_size: 0,
            media_type,
        }
    }
}

impl MonitoredStreamingUploader {
    pub async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        let operation_type = OperationType::StreamingUpload;
        let operation_type_str = operation_type.as_str();
        let data_len = data.len();

        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type, operation_type_str])
            .start_timer();

        let res = self
            .inner
            .write_bytes(data)
            .verbose_instrument_await(operation_type_str)
            .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);

        self.object_store_metrics
            .write_bytes
            .inc_by(data_len as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(data_len as f64);
        self.operation_size += data_len;

        res
    }

    pub async fn finish(self) -> ObjectResult<()> {
        let operation_type = OperationType::StreamingUploadFinish;
        let operation_type_str = operation_type.as_str();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type, operation_type_str])
            .start_timer();

        let res = self
            .inner
            .finish()
            .verbose_instrument_await(operation_type_str)
            .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(self.operation_size as f64);
        res
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.inner.get_memory_usage()
    }
}

pub struct MonitoredStreamingReader {
    inner: ObjectDataStream,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    operation_size: usize,
    media_type: &'static str,
    streaming_read_timeout: Option<Duration>,
    operation_type_str: &'static str,
}

impl MonitoredStreamingReader {
    pub fn new(
        media_type: &'static str,
        handle: ObjectDataStream,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        streaming_read_timeout: Option<Duration>,
    ) -> Self {
        Self {
            inner: handle,
            object_store_metrics,
            operation_size: 0,
            media_type,
            streaming_read_timeout,
            operation_type_str: OperationType::StreamingRead.as_str(),
        }
    }

    pub async fn read_bytes(&mut self) -> Option<ObjectResult<Bytes>> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type, self.operation_type_str])
            .start_timer();
        let future = async {
            self.inner
                .next()
                .verbose_instrument_await(self.operation_type_str)
                .await
        };
        let res = match self.streaming_read_timeout.as_ref() {
            None => future.await,
            Some(timeout_duration) => tokio::time::timeout(*timeout_duration, future)
                .await
                .unwrap_or_else(|_| {
                    Some(Err(ObjectError::timeout(format!(
                        "Retry attempts exhausted for {}. Please modify {}_attempt_timeout_ms (current={:?}) under [storage.object_store.retry] in the config accordingly if needed.",
                        self.operation_type_str, self.operation_type_str, timeout_duration.as_millis()
                    ))))
                }),
        };

        if let Some(ret) = &res {
            try_update_failure_metric(&self.object_store_metrics, ret, self.operation_type_str);
        }
        if let Some(Ok(data)) = &res {
            let data_len = data.len();
            self.object_store_metrics.read_bytes.inc_by(data_len as u64);
            self.object_store_metrics
                .operation_size
                .with_label_values(&[self.operation_type_str])
                .observe(data_len as f64);
            self.operation_size += data_len;
        }
        res
    }
}

impl Drop for MonitoredStreamingReader {
    fn drop(&mut self) {
        self.object_store_metrics
            .operation_size
            .with_label_values(&[self.operation_type_str])
            .observe(self.operation_size as f64);
    }
}

pub struct MonitoredObjectStore<OS: ObjectStore> {
    inner: OS,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    config: Arc<ObjectStoreConfig>,
}

/// Manually dispatch trait methods.
///
/// The metrics are updated in the following order:
/// - Write operations
///   - `write_bytes`
///   - `operation_size`
///   - start `operation_latency` timer
///   - `failure_count`
/// - Read operations
///   - start `operation_latency` timer
///   - `failure-count`
///   - `read_bytes`
///   - `operation_size`
/// - Other
///   - start `operation_latency` timer
///   - `failure-count`
impl<OS: ObjectStore> MonitoredObjectStore<OS> {
    pub fn new(
        store: OS,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        config: Arc<ObjectStoreConfig>,
    ) -> Self {
        Self {
            object_store_metrics,
            inner: store,
            config,
        }
    }

    fn media_type(&self) -> &'static str {
        self.inner.store_media_type()
    }

    pub fn inner(&self) -> &OS {
        &self.inner
    }

    pub fn mut_inner(&mut self) -> &mut OS {
        &mut self.inner
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        let operation_type = OperationType::Upload;
        let operation_type_str = operation_type.as_str();

        self.object_store_metrics
            .write_bytes
            .inc_by(obj.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(obj.len() as f64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .upload(path, obj.clone())
                .verbose_instrument_await(operation_type_str)
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    pub async fn streaming_upload(&self, path: &str) -> ObjectResult<MonitoredStreamingUploader> {
        let operation_type = OperationType::StreamingUploadInit;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let res = self
            .inner
            .streaming_upload(path)
            .verbose_instrument_await(operation_type_str)
            .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        Ok(MonitoredStreamingUploader::new(
            media_type,
            res?,
            self.object_store_metrics.clone(),
        ))
    }

    pub async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let operation_type = OperationType::Read;
        let operation_type_str = operation_type.as_str();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .read(path, range.clone())
                .verbose_instrument_await(operation_type_str)
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
        )
        .await;

        if let Err(e) = &res
            && e.is_object_not_found_error()
            && !path.ends_with(".data")
        {
            // Some not_found_error is expected, e.g. metadata backup's manifest.json.
            // This is a quick fix that'll only log error in `try_update_failure_metric` in state store usage.
        } else {
            try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        }

        let data = res?;
        self.object_store_metrics
            .read_bytes
            .inc_by(data.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type_str])
            .observe(data.len() as f64);
        Ok(data)
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        range: Range<usize>,
    ) -> ObjectResult<MonitoredStreamingReader> {
        let operation_type = OperationType::StreamingReadInit;
        let operation_type_str = operation_type.as_str();
        let media_type = self.media_type();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .streaming_read(path, range.clone())
                .verbose_instrument_await(operation_type_str)
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);

        Ok(MonitoredStreamingReader::new(
            media_type,
            res?,
            self.object_store_metrics.clone(),
            Some(Duration::from_millis(
                self.config.retry.streaming_read_attempt_timeout_ms,
            )),
        ))
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let operation_type = OperationType::Metadata;
        let operation_type_str = operation_type.as_str();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .metadata(path)
                .verbose_instrument_await(operation_type_str)
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        let operation_type = OperationType::Delete;
        let operation_type_str = operation_type.as_str();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .delete(path)
                .verbose_instrument_await(operation_type_str)
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        let operation_type = OperationType::DeleteObjects;
        let operation_type_str = operation_type.as_str();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .delete_objects(paths)
                .verbose_instrument_await(operation_type_str)
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }

    pub async fn list(&self, prefix: &str) -> ObjectResult<ObjectMetadataIter> {
        let operation_type = OperationType::List;
        let operation_type_str = operation_type.as_str();

        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type_str])
            .start_timer();

        let builder = || async {
            self.inner
                .list(prefix)
                .verbose_instrument_await(operation_type_str)
                .await
        };

        let res = retry_request(
            builder,
            &self.config,
            operation_type,
            self.object_store_metrics.clone(),
        )
        .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type_str);
        res
    }
}

/// Creates a new [`ObjectStore`] from the given `url`. Credentials are configured via environment
/// variables.
///
/// # Panics
/// If the `url` is invalid. Therefore, it is only suitable to be used during startup.
pub async fn build_remote_object_store(
    url: &str,
    metrics: Arc<ObjectStoreMetrics>,
    ident: &str,
    config: Arc<ObjectStoreConfig>,
) -> ObjectStoreImpl {
    tracing::debug!(config=?config, "object store {ident}");
    match url {
        s3 if s3.starts_with("s3://") => {
            if config.s3.developer.use_opendal {
                let bucket = s3.strip_prefix("s3://").unwrap();
                tracing::info!("Using OpenDAL to access s3, bucket is {}", bucket);
                ObjectStoreImpl::Opendal(
                    OpendalObjectStore::new_s3_engine(bucket.to_string(), config.clone())
                        .unwrap()
                        .monitored(metrics, config),
                )
            } else {
                ObjectStoreImpl::S3(
                    S3ObjectStore::new_with_config(
                        s3.strip_prefix("s3://").unwrap().to_string(),
                        metrics.clone(),
                        config.clone(),
                    )
                    .await
                    .monitored(metrics, config),
                )
            }
        }
        #[cfg(feature = "hdfs-backend")]
        hdfs if hdfs.starts_with("hdfs://") => {
            let hdfs = hdfs.strip_prefix("hdfs://").unwrap();
            let (namenode, root) = hdfs.split_once('@').unwrap_or((hdfs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_hdfs_engine(
                    namenode.to_string(),
                    root.to_string(),
                    config.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        gcs if gcs.starts_with("gcs://") => {
            let gcs = gcs.strip_prefix("gcs://").unwrap();
            let (bucket, root) = gcs.split_once('@').unwrap_or((gcs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_gcs_engine(
                    bucket.to_string(),
                    root.to_string(),
                    config.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        obs if obs.starts_with("obs://") => {
            let obs = obs.strip_prefix("obs://").unwrap();
            let (bucket, root) = obs.split_once('@').unwrap_or((obs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_obs_engine(
                    bucket.to_string(),
                    root.to_string(),
                    config.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }

        oss if oss.starts_with("oss://") => {
            let oss = oss.strip_prefix("oss://").unwrap();
            let (bucket, root) = oss.split_once('@').unwrap_or((oss, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_oss_engine(
                    bucket.to_string(),
                    root.to_string(),
                    config.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        webhdfs if webhdfs.starts_with("webhdfs://") => {
            let webhdfs = webhdfs.strip_prefix("webhdfs://").unwrap();
            let (namenode, root) = webhdfs.split_once('@').unwrap_or((webhdfs, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_webhdfs_engine(
                    namenode.to_string(),
                    root.to_string(),
                    config.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        azblob if azblob.starts_with("azblob://") => {
            let azblob = azblob.strip_prefix("azblob://").unwrap();
            let (container_name, root) = azblob.split_once('@').unwrap_or((azblob, ""));
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_azblob_engine(
                    container_name.to_string(),
                    root.to_string(),
                    config.clone(),
                )
                .unwrap()
                .monitored(metrics, config),
            )
        }
        fs if fs.starts_with("fs://") => {
            let fs = fs.strip_prefix("fs://").unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_fs_engine(fs.to_string(), config.clone())
                    .unwrap()
                    .monitored(metrics, config),
            )
        }

        s3_compatible if s3_compatible.starts_with("s3-compatible://") => {
            tracing::error!("The s3 compatible mode has been unified with s3.");
            tracing::error!("If you want to use s3 compatible storage, please set your access_key, secret_key and region to the environment variable AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
            set your endpoint to the environment variable RW_S3_ENDPOINT.");
            panic!("Passing s3-compatible is not supported, please modify the environment variable and pass in s3.");
        }
        minio if minio.starts_with("minio://") => {
            if config.s3.developer.use_opendal {
                tracing::info!("Using OpenDAL to access minio.");
                ObjectStoreImpl::Opendal(
                    OpendalObjectStore::new_minio_engine(minio, config.clone())
                        .unwrap()
                        .monitored(metrics, config),
                )
            } else {
                ObjectStoreImpl::S3(
                    S3ObjectStore::new_minio_engine(minio, metrics.clone(), config.clone())
                        .await
                        .monitored(metrics, config),
                )
            }
        }
        "memory" => {
            if ident == "Meta Backup" {
                tracing::warn!("You're using in-memory remote object store for {}. This is not recommended for production environment.", ident);
            } else {
                tracing::warn!("You're using in-memory remote object store for {}. This should never be used in benchmarks and production environment.", ident);
            }
            ObjectStoreImpl::InMem(InMemObjectStore::new().monitored(metrics, config))
        }
        "memory-shared" => {
            if ident == "Meta Backup" {
                tracing::warn!("You're using shared in-memory remote object store for {}. This should never be used in production environment.", ident);
            } else {
                tracing::warn!("You're using shared in-memory remote object store for {}. This should never be used in benchmarks and production environment.", ident);
            }
            ObjectStoreImpl::InMem(InMemObjectStore::shared().monitored(metrics, config))
        }
        #[cfg(madsim)]
        sim if sim.starts_with("sim://") => {
            ObjectStoreImpl::Sim(SimObjectStore::new(url).monitored(metrics, config))
        }
        other => {
            unimplemented!(
                "{} remote object store only supports s3, minio, gcs, oss, cos, azure blob, hdfs, disk, memory, and memory-shared.",
                other
            )
        }
    }
}

#[inline(always)]
fn get_retry_strategy(
    config: &ObjectStoreConfig,
    operation_type: OperationType,
) -> impl Iterator<Item = Duration> {
    let attempts = get_retry_attempts_by_type(config, operation_type);
    ExponentialBackoff::from_millis(config.retry.req_backoff_interval_ms)
        .max_delay(Duration::from_millis(config.retry.req_backoff_max_delay_ms))
        .factor(config.retry.req_backoff_factor)
        .take(attempts)
        .map(jitter)
}

pub type ObjectMetadataIter = BoxStream<'static, ObjectResult<ObjectMetadata>>;
pub type ObjectDataStream = BoxStream<'static, ObjectResult<Bytes>>;

#[derive(Debug, Clone, Copy)]
enum OperationType {
    Upload,
    StreamingUploadInit,
    StreamingUpload,
    StreamingUploadFinish,
    Read,
    StreamingReadInit,
    StreamingRead,
    Metadata,
    Delete,
    DeleteObjects,
    List,
}

impl OperationType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::StreamingUploadInit => "streaming_upload_init",
            Self::StreamingUpload => "streaming_upload",
            Self::StreamingUploadFinish => "streaming_upload_finish",
            Self::Read => "read",
            Self::StreamingReadInit => "streaming_read_init",
            Self::StreamingRead => "streaming_read",
            Self::Metadata => "metadata",
            Self::Delete => "delete",
            Self::DeleteObjects => "delete_objects",
            Self::List => "list",
        }
    }
}

fn get_retry_attempts_by_type(config: &ObjectStoreConfig, operation_type: OperationType) -> usize {
    match operation_type {
        OperationType::Upload => config.retry.upload_retry_attempts,
        OperationType::StreamingUploadInit
        | OperationType::StreamingUpload
        | OperationType::StreamingUploadFinish => config.retry.streaming_upload_retry_attempts,
        OperationType::Read => config.retry.read_retry_attempts,
        OperationType::StreamingReadInit | OperationType::StreamingRead => {
            config.retry.streaming_read_retry_attempts
        }
        OperationType::Metadata => config.retry.metadata_retry_attempts,
        OperationType::Delete => config.retry.delete_retry_attempts,
        OperationType::DeleteObjects => config.retry.delete_objects_retry_attempts,
        OperationType::List => config.retry.list_retry_attempts,
    }
}

fn get_attempt_timeout_by_type(config: &ObjectStoreConfig, operation_type: OperationType) -> u64 {
    match operation_type {
        OperationType::Upload => config.retry.upload_attempt_timeout_ms,
        OperationType::StreamingUploadInit
        | OperationType::StreamingUpload
        | OperationType::StreamingUploadFinish => config.retry.streaming_upload_attempt_timeout_ms,
        OperationType::Read => config.retry.read_attempt_timeout_ms,
        OperationType::StreamingReadInit | OperationType::StreamingRead => {
            config.retry.streaming_read_attempt_timeout_ms
        }
        OperationType::Metadata => config.retry.metadata_attempt_timeout_ms,
        OperationType::Delete => config.retry.delete_attempt_timeout_ms,
        OperationType::DeleteObjects => config.retry.delete_attempt_timeout_ms,
        OperationType::List => config.retry.list_attempt_timeout_ms,
    }
}

struct RetryCondition {
    operation_type: OperationType,
    retry_count: usize,
    metrics: Arc<ObjectStoreMetrics>,
}

impl RetryCondition {
    fn new(operation_type: OperationType, metrics: Arc<ObjectStoreMetrics>) -> Self {
        Self {
            operation_type,
            retry_count: 0,
            metrics,
        }
    }

    #[inline(always)]
    fn should_retry_inner(&mut self, err: &ObjectError) -> bool {
        let should_retry = err.should_retry();
        if should_retry {
            self.retry_count += 1;
        }

        should_retry
    }
}

impl tokio_retry::Condition<ObjectError> for RetryCondition {
    fn should_retry(&mut self, err: &ObjectError) -> bool {
        self.should_retry_inner(err)
    }
}

impl Drop for RetryCondition {
    fn drop(&mut self) {
        if self.retry_count > 0 {
            self.metrics
                .request_retry_count
                .with_label_values(&[self.operation_type.as_str()])
                .inc_by(self.retry_count as _);
        }
    }
}

async fn retry_request<F, T, B>(
    builder: B,
    config: &ObjectStoreConfig,
    operation_type: OperationType,
    object_store_metrics: Arc<ObjectStoreMetrics>,
) -> ObjectResult<T>
where
    B: Fn() -> F,
    F: Future<Output = ObjectResult<T>>,
{
    let backoff = get_retry_strategy(config, operation_type);
    let timeout_duration =
        Duration::from_millis(get_attempt_timeout_by_type(config, operation_type));
    let operation_type_str = operation_type.as_str();

    let retry_condition = RetryCondition::new(operation_type, object_store_metrics);

    let f = || async {
        let future = builder();
        if timeout_duration.is_zero() {
            future.await
        } else {
            tokio::time::timeout(timeout_duration, future)
                .await
                .unwrap_or_else(|_| {
                    Err(ObjectError::timeout(format!(
                        "Retry attempts exhausted for {}. Please modify {}_attempt_timeout_ms (current={:?}) and {}_retry_attempts (current={}) under [storage.object_store.retry] in the config accordingly if needed.",
                        operation_type_str, operation_type_str, timeout_duration.as_millis(), operation_type_str, get_retry_attempts_by_type(config, operation_type)
                    )))
                })
        }
    };

    tokio_retry::RetryIf::spawn(backoff, f, retry_condition).await
}
