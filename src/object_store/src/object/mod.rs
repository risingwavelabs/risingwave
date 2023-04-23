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

use bytes::Bytes;
use prometheus::HistogramTimer;
use tokio::io::{AsyncRead, AsyncReadExt};

pub mod mem;
pub use mem::*;

pub mod opendal_engine;
pub use opendal_engine::*;

pub mod s3;
use await_tree::InstrumentAwait;
pub use s3::*;

mod disk;
pub mod error;
pub mod object_metrics;

pub use error::*;
use object_metrics::ObjectStoreMetrics;

use crate::object::disk::DiskObjectStore;

pub const LOCAL_OBJECT_STORE_PATH_PREFIX: &str = "@local:";

pub type ObjectStoreRef = Arc<ObjectStoreImpl>;
pub type ObjectStreamingUploader = MonitoredStreamingUploader;

type BoxedStreamingUploader = Box<dyn StreamingUploader>;

#[derive(Debug)]
pub enum ObjectStorePath<'a> {
    Local(&'a str),
    Remote(&'a str),
}

impl ObjectStorePath<'_> {
    pub fn is_local(&self) -> bool {
        match self {
            ObjectStorePath::Local(_) => true,
            ObjectStorePath::Remote(_) => false,
        }
    }

    pub fn is_remote(&self) -> bool {
        !self.is_local()
    }

    pub fn as_str(&self) -> &str {
        match self {
            ObjectStorePath::Local(path) => path,
            ObjectStorePath::Remote(path) => path,
        }
    }
}

pub fn get_local_path(path: &str) -> String {
    LOCAL_OBJECT_STORE_PATH_PREFIX.to_string() + path
}

pub fn parse_object_store_path(path: &str) -> ObjectStorePath<'_> {
    match path.strip_prefix(LOCAL_OBJECT_STORE_PATH_PREFIX) {
        Some(path) => ObjectStorePath::Local(path),
        None => ObjectStorePath::Remote(path),
    }
}

/// Partitions a set of given paths into two vectors. The first vector contains all local paths, and
/// the second contains all remote paths.
pub fn partition_object_store_paths(paths: &[String]) -> (Vec<String>, Vec<String>) {
    // ToDo: Currently the result is a copy of the input. Would it be worth it to use an in-place
    //       partition instead?
    let mut vec_loc = vec![];
    let mut vec_rem = vec![];

    for path in paths {
        match path.strip_prefix(LOCAL_OBJECT_STORE_PATH_PREFIX) {
            Some(path) => vec_loc.push(path.to_string()),
            None => vec_rem.push(path.to_string()),
        };
    }

    (vec_loc, vec_rem)
}

#[derive(Debug, Copy, Clone)]
pub struct BlockLocation {
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMetadata {
    // Full path
    pub key: String,
    // Seconds since unix epoch.
    pub last_modified: f64,
    pub total_size: usize,
}

impl BlockLocation {
    /// Generates the http bytes range specifier.
    pub fn byte_range_specifier(&self) -> Option<String> {
        Some(format!(
            "bytes={}-{}",
            self.offset,
            self.offset + self.size - 1
        ))
    }
}

#[async_trait::async_trait]
pub trait StreamingUploader: Send + Sync {
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

    /// If the `block_loc` is None, the whole object will be returned.
    /// If objects are PUT using a multipart upload, it’s a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// <https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2>
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes>;

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>>;

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        start_pos: Option<usize>,
    ) -> ObjectResult<Box<dyn AsyncRead + Unpin + Send + Sync>>;

    /// Obtains the object metadata.
    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata>;

    /// Deletes blob permanently.
    async fn delete(&self, path: &str) -> ObjectResult<()>;

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()>;

    fn monitored(self, metrics: Arc<ObjectStoreMetrics>) -> MonitoredObjectStore<Self>
    where
        Self: Sized,
    {
        MonitoredObjectStore::new(self, metrics)
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>>;

    fn store_media_type(&self) -> &'static str;
}

pub enum ObjectStoreImpl {
    InMem(MonitoredObjectStore<InMemObjectStore>),
    Disk(MonitoredObjectStore<DiskObjectStore>),
    Opendal(MonitoredObjectStore<OpendalObjectStore>),
    S3(MonitoredObjectStore<S3ObjectStore>),
    S3Compatible(MonitoredObjectStore<S3ObjectStore>),
    Hybrid {
        local: Box<ObjectStoreImpl>,
        remote: Box<ObjectStoreImpl>,
    },
}

impl ObjectStoreImpl {
    pub fn hybrid(local: ObjectStoreImpl, remote: ObjectStoreImpl) -> Self {
        ObjectStoreImpl::Hybrid {
            local: Box::new(local),
            remote: Box::new(remote),
        }
    }
}

macro_rules! dispatch_async {
    ($object_store:expr, $method_name:ident $(, $args:expr)*) => {
        $object_store.$method_name($($args, )*).await
    }
}

/// This macro routes the object store operation to the real implementation by the `ObjectStoreImpl`
/// enum type and the `path`.
///
/// For `path`, if the `path` starts with `LOCAL_OBJECT_STORE_PATH_PREFIX`, it indicates that the
/// operation should be performed on the local object store, and otherwise the operation should be
/// performed on remote object store.
macro_rules! object_store_impl_method_body {
    ($object_store:expr, $method_name:ident, $dispatch_macro:ident, $path:expr $(, $args:expr)*) => {
        {
            let path = parse_object_store_path($path);
            match $object_store {
                ObjectStoreImpl::InMem(in_mem) => {
                    assert!(path.is_remote(), "get local path in pure in-mem object store: {:?}", $path);
                    $dispatch_macro!(in_mem, $method_name, path.as_str() $(, $args)*)
                },
                ObjectStoreImpl::Disk(disk) => {
                    assert!(path.is_remote(), "get local path in pure disk object store: {:?}", $path);
                    $dispatch_macro!(disk, $method_name, path.as_str() $(, $args)*)
                },
                ObjectStoreImpl::Opendal(opendal) => {
                    assert!(path.is_remote(), "get local path in pure opendal object store engine: {:?}", $path);
                    $dispatch_macro!(opendal, $method_name, path.as_str() $(, $args)*)
                },
                ObjectStoreImpl::S3(s3) => {
                    assert!(path.is_remote(), "get local path in pure s3 object store: {:?}", $path);
                    $dispatch_macro!(s3, $method_name, path.as_str() $(, $args)*)
                },
                ObjectStoreImpl::S3Compatible(s3) => {
                    assert!(path.is_remote(), "get local path in pure s3 compatible object store: {:?}", $path);
                    $dispatch_macro!(s3, $method_name, path.as_str() $(, $args)*)
                },
                ObjectStoreImpl::Hybrid {
                    local: local,
                    remote: remote,
                } => {
                    match path {
                        ObjectStorePath::Local(_) => match local.as_ref() {
                            ObjectStoreImpl::InMem(in_mem) => $dispatch_macro!(in_mem, $method_name, path.as_str() $(, $args)*),
                            ObjectStoreImpl::Disk(disk) => $dispatch_macro!(disk, $method_name, path.as_str() $(, $args)*),
                            ObjectStoreImpl::Opendal(_) => unreachable!("Opendal object store cannot be used as local object store"),
                            ObjectStoreImpl::S3(_) => unreachable!("S3 cannot be used as local object store"),
                            ObjectStoreImpl::S3Compatible(_) => unreachable!("S3 compatible cannot be used as local object store"),
                            ObjectStoreImpl::Hybrid {..} => unreachable!("local object store of hybrid object store cannot be hybrid")
                        },
                        ObjectStorePath::Remote(_) => match remote.as_ref() {
                            ObjectStoreImpl::InMem(in_mem) => $dispatch_macro!(in_mem, $method_name, path.as_str() $(, $args)*),
                            ObjectStoreImpl::Disk(disk) => $dispatch_macro!(disk, $method_name, path.as_str() $(, $args)*),
                            ObjectStoreImpl::Opendal(opendal) => $dispatch_macro!(opendal, $method_name, path.as_str() $(, $args)*),
                            ObjectStoreImpl::S3(s3) => $dispatch_macro!(s3, $method_name, path.as_str() $(, $args)*),
                            ObjectStoreImpl::S3Compatible(s3_compatible) => $dispatch_macro!(s3_compatible, $method_name, path.as_str() $(, $args)*),
                            ObjectStoreImpl::Hybrid {..} => unreachable!("remote object store of hybrid object store cannot be hybrid")
                        },
                    }
                }
            }
        }
    };
}

/// This macro routes the object store operation to the real implementation by the `ObjectStoreImpl`
/// enum type and the `paths`. It is a modification of the macro above to work with a slice of
/// strings instead of just a single one.
///
/// If an entry in `paths` starts with `LOCAL_OBJECT_STORE_PATH_PREFIX`, it indicates that the
/// operation should be performed on the local object store, and otherwise the operation should be
/// performed on remote object store.
macro_rules! object_store_impl_method_body_slice {
    ($object_store:expr, $method_name:ident, $dispatch_macro:ident, $paths:expr $(, $args:expr)*) => {
        {
            let (paths_loc, paths_rem) = partition_object_store_paths($paths);
            match $object_store {
                ObjectStoreImpl::InMem(in_mem) => {
                    assert!(paths_loc.is_empty(), "get local path in pure in-mem object store: {:?}", $paths);
                    $dispatch_macro!(in_mem, $method_name, &paths_rem $(, $args)*)
                },
                ObjectStoreImpl::Disk(disk) => {
                    assert!(paths_loc.is_empty(), "get local path in pure disk object store: {:?}", $paths);
                    $dispatch_macro!(disk, $method_name, &paths_rem $(, $args)*)
                },
                ObjectStoreImpl::Opendal(opendal) => {
                    assert!(paths_loc.is_empty(), "get local path in pure opendal object store: {:?}", $paths);
                    $dispatch_macro!(opendal, $method_name, &paths_rem $(, $args)*)
                },
                ObjectStoreImpl::S3(s3) => {
                    assert!(paths_loc.is_empty(), "get local path in pure s3 object store: {:?}", $paths);
                    $dispatch_macro!(s3, $method_name, &paths_rem $(, $args)*)
                },
                ObjectStoreImpl::S3Compatible(s3) => {
                    assert!(paths_loc.is_empty(), "get local path in pure s3 compatible object store: {:?}", $paths);
                    $dispatch_macro!(s3, $method_name, &paths_rem $(, $args)*)
                },
                ObjectStoreImpl::Hybrid {
                    local: local,
                    remote: remote,
                } => {
                    // Process local paths.
                    match local.as_ref() {
                        ObjectStoreImpl::InMem(in_mem) =>  $dispatch_macro!(in_mem, $method_name, &paths_loc $(, $args)*),
                        ObjectStoreImpl::Disk(disk) =>  $dispatch_macro!(disk, $method_name, &paths_loc $(, $args)*),
                        ObjectStoreImpl::Opendal(_) => unreachable!("Opendal object store cannot be used as local object store"),
                        ObjectStoreImpl::S3(_) => unreachable!("S3 cannot be used as local object store"),
                        ObjectStoreImpl::S3Compatible(_) => unreachable!("S3 cannot be used as local object store"),
                        ObjectStoreImpl::Hybrid {..} => unreachable!("local object store of hybrid object store cannot be hybrid")
                    }?;

                    // Process remote paths.
                    match remote.as_ref() {
                        ObjectStoreImpl::InMem(in_mem) =>  $dispatch_macro!(in_mem, $method_name, &paths_rem $(, $args)*),
                        ObjectStoreImpl::Disk(disk) =>  $dispatch_macro!(disk, $method_name, &paths_rem $(, $args)*),
                        ObjectStoreImpl::Opendal(opendal) =>  $dispatch_macro!(opendal, $method_name, &paths_rem $(, $args)*),
                        ObjectStoreImpl::S3(s3) =>  $dispatch_macro!(s3, $method_name, &paths_rem $(, $args)*),
                        ObjectStoreImpl::S3Compatible(s3) =>  $dispatch_macro!(s3, $method_name, &paths_rem $(, $args)*),
                        ObjectStoreImpl::Hybrid {..} => unreachable!("remote object store of hybrid object store cannot be hybrid")
                    }
                }
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

    pub async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        object_store_impl_method_body!(self, read, dispatch_async, path, block_loc)
    }

    pub async fn readv(
        &self,
        path: &str,
        block_locs: &[BlockLocation],
    ) -> ObjectResult<Vec<Bytes>> {
        object_store_impl_method_body!(self, readv, dispatch_async, path, block_locs)
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
        start_loc: Option<usize>,
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

    pub async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        object_store_impl_method_body!(self, list, dispatch_async, prefix)
    }

    pub fn get_object_prefix(&self, obj_id: u64, is_remote: bool) -> String {
        // FIXME: ObjectStoreImpl lacks flexibility for adding new interface to ObjectStore
        // trait. Macro object_store_impl_method_body routes to local or remote only depending on
        // the path
        match self {
            ObjectStoreImpl::InMem(store) => store.inner.get_object_prefix(obj_id),
            ObjectStoreImpl::Disk(store) => store.inner.get_object_prefix(obj_id),
            ObjectStoreImpl::Opendal(store) => store.inner.get_object_prefix(obj_id),
            ObjectStoreImpl::S3(store) => store.inner.get_object_prefix(obj_id),
            ObjectStoreImpl::S3Compatible(store) => store.inner.get_object_prefix(obj_id),
            ObjectStoreImpl::Hybrid { local, remote } => {
                if is_remote {
                    remote.get_object_prefix(obj_id, true)
                } else {
                    local.get_object_prefix(obj_id, false)
                }
            }
        }
    }
}

fn try_update_failure_metric<T>(
    metrics: &Arc<ObjectStoreMetrics>,
    result: &ObjectResult<T>,
    operation_type: &'static str,
) {
    if result.is_err() {
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
        let operation_type = "streaming_upload_write_bytes";
        let data_len = data.len();
        self.object_store_metrics
            .write_bytes
            .inc_by(data.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type])
            .observe(data_len as f64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type, operation_type])
            .start_timer();
        self.operation_size += data_len;

        let ret = self.inner.write_bytes(data).await;

        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }

    pub async fn finish(self) -> ObjectResult<()> {
        let operation_type = "streaming_upload_finish";
        self.object_store_metrics
            .operation_size
            .with_label_values(&["streaming_upload"])
            .observe(self.operation_size as f64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type, operation_type])
            .start_timer();

        let ret = self.inner.finish().await;

        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.inner.get_memory_usage()
    }
}

type BoxedStreamingReader = Box<dyn AsyncRead + Unpin + Send + Sync>;
pub struct MonitoredStreamingReader {
    inner: BoxedStreamingReader,
    object_store_metrics: Arc<ObjectStoreMetrics>,
    operation_size: usize,
    media_type: &'static str,
    timer: Option<HistogramTimer>,
}

impl MonitoredStreamingReader {
    pub fn new(
        media_type: &'static str,
        handle: BoxedStreamingReader,
        object_store_metrics: Arc<ObjectStoreMetrics>,
    ) -> Self {
        let operation_type = "streaming_read";
        let timer = object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type])
            .start_timer();
        Self {
            inner: handle,
            object_store_metrics,
            operation_size: 0,
            media_type,
            timer: Some(timer),
        }
    }

    pub async fn read_bytes(&mut self, buf: &mut [u8]) -> ObjectResult<usize> {
        let operation_type = "streaming_read_read_bytes";
        let data_len = buf.len();
        self.object_store_metrics.read_bytes.inc_by(data_len as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type])
            .observe(data_len as f64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type, operation_type])
            .start_timer();
        self.operation_size += data_len;
        let ret =
            self.inner.read_exact(buf).await.map_err(|err| {
                ObjectError::internal(format!("read_bytes failed, error: {:?}", err))
            });
        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }
}

impl Drop for MonitoredStreamingReader {
    fn drop(&mut self) {
        let operation_type = "streaming_read";
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type])
            .observe(self.operation_size as f64);
        self.timer.take().unwrap().observe_duration();
    }
}

pub struct MonitoredObjectStore<OS: ObjectStore> {
    inner: OS,
    object_store_metrics: Arc<ObjectStoreMetrics>,
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
    pub fn new(store: OS, object_store_metrics: Arc<ObjectStoreMetrics>) -> Self {
        Self {
            inner: store,
            object_store_metrics,
        }
    }

    fn media_type(&self) -> &'static str {
        self.inner.store_media_type()
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        let operation_type = "upload";
        self.object_store_metrics
            .write_bytes
            .inc_by(obj.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type])
            .observe(obj.len() as f64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type])
            .start_timer();

        let ret = self
            .inner
            .upload(path, obj)
            .verbose_instrument_await("object_store_upload")
            .await;

        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }

    pub async fn streaming_upload(&self, path: &str) -> ObjectResult<MonitoredStreamingUploader> {
        let operation_type = "streaming_upload_start";
        let media_type = self.media_type();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type])
            .start_timer();

        let handle_res = self.inner.streaming_upload(path).await;

        try_update_failure_metric(&self.object_store_metrics, &handle_res, operation_type);
        Ok(MonitoredStreamingUploader::new(
            media_type,
            handle_res?,
            self.object_store_metrics.clone(),
        ))
    }

    pub async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        let operation_type = "read";
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type])
            .start_timer();

        let res = self
            .inner
            .read(path, block_loc)
            .verbose_instrument_await("object_store_read")
            .await
            .map_err(|err| {
                ObjectError::internal(format!(
                    "read {:?} in block {:?} failed, error: {:?}",
                    path, block_loc, err
                ))
            });

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type);

        let data = res?;
        self.object_store_metrics
            .read_bytes
            .inc_by(data.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type])
            .observe(data.len() as f64);
        Ok(data)
    }

    pub async fn readv(
        &self,
        path: &str,
        block_locs: &[BlockLocation],
    ) -> ObjectResult<Vec<Bytes>> {
        let operation_type = "readv";
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type])
            .start_timer();

        let res = self
            .inner
            .readv(path, block_locs)
            .verbose_instrument_await("object_store_readv")
            .await;

        try_update_failure_metric(&self.object_store_metrics, &res, operation_type);

        let data = res?;
        let data_len = data.iter().map(|block| block.len()).sum::<usize>() as u64;
        self.object_store_metrics.read_bytes.inc_by(data_len);
        self.object_store_metrics
            .operation_size
            .with_label_values(&[operation_type])
            .observe(data_len as f64);
        Ok(data)
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        start_pos: Option<usize>,
    ) -> ObjectResult<MonitoredStreamingReader> {
        let operation_type = "streaming_read_start";
        let media_type = self.media_type();
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[media_type, operation_type])
            .start_timer();
        let ret = self.inner.streaming_read(path, start_pos).await;
        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        Ok(MonitoredStreamingReader::new(
            media_type,
            ret?,
            self.object_store_metrics.clone(),
        ))
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let operation_type = "metadata";
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type])
            .start_timer();

        let ret = self
            .inner
            .metadata(path)
            .verbose_instrument_await("object_store_metadata")
            .await;

        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        let operation_type = "delete";
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type])
            .start_timer();

        let ret = self
            .inner
            .delete(path)
            .verbose_instrument_await("object_store_delete")
            .await;

        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }

    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        let operation_type = "delete_objects";
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type])
            .start_timer();

        let ret = self
            .inner
            .delete_objects(paths)
            .verbose_instrument_await("object_store_delete_objects")
            .await;

        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }

    pub async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        let operation_type = "list";
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&[self.media_type(), operation_type])
            .start_timer();

        let ret = self
            .inner
            .list(prefix)
            .verbose_instrument_await("object_store_list")
            .await;

        try_update_failure_metric(&self.object_store_metrics, &ret, operation_type);
        ret
    }
}

pub async fn parse_remote_object_store(
    url: &str,
    metrics: Arc<ObjectStoreMetrics>,
    ident: &str,
) -> ObjectStoreImpl {
    match url {
        s3 if s3.starts_with("s3://") => ObjectStoreImpl::S3(
            S3ObjectStore::new(
                s3.strip_prefix("s3://").unwrap().to_string(),
                metrics.clone(),
            )
            .await
            .monitored(metrics),
        ),
        #[cfg(feature = "hdfs-backend")]
        hdfs if hdfs.starts_with("hdfs://") => {
            let hdfs = hdfs.strip_prefix("hdfs://").unwrap();
            let (namenode, root) = hdfs.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_hdfs_engine(namenode.to_string(), root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }
        gcs if gcs.starts_with("gcs://") => {
            let gcs = gcs.strip_prefix("gcs://").unwrap();
            let (bucket, root) = gcs.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_gcs_engine(bucket.to_string(), root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }

        oss if oss.starts_with("oss://") => {
            let oss = oss.strip_prefix("oss://").unwrap();
            let (bucket, root) = oss.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_oss_engine(bucket.to_string(), root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }
        webhdfs if webhdfs.starts_with("webhdfs://") => {
            let webhdfs = webhdfs.strip_prefix("webhdfs://").unwrap();
            let (endpoint, root) = webhdfs.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_webhdfs_engine(endpoint.to_string(), root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }
        azblob if azblob.starts_with("azblob://") => {
            let azblob = azblob.strip_prefix("azblob://").unwrap();
            let (container_name, root) = azblob.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_azblob_engine(container_name.to_string(), root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }
        fs if fs.starts_with("fs://") => {
            let fs = fs.strip_prefix("fs://").unwrap();
            let (_, root) = fs.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_fs_engine(root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }
        s3_compatible if s3_compatible.starts_with("s3-compatible://") => {
            ObjectStoreImpl::S3Compatible(
                S3ObjectStore::new_s3_compatible(
                    s3_compatible
                        .strip_prefix("s3-compatible://")
                        .unwrap()
                        .to_string(),
                    metrics.clone(),
                )
                .await
                .monitored(metrics),
            )
        }
        minio if minio.starts_with("minio://") => ObjectStoreImpl::S3(
            S3ObjectStore::with_minio(minio, metrics.clone())
                .await
                .monitored(metrics),
        ),
        disk if disk.starts_with("disk://") => ObjectStoreImpl::Disk(
            DiskObjectStore::new(disk.strip_prefix("disk://").unwrap()).monitored(metrics),
        ),
        "memory" => {
            tracing::warn!("You're using in-memory remote object store for {}. This should never be used in benchmarks and production environment.", ident);
            ObjectStoreImpl::InMem(InMemObjectStore::new().monitored(metrics))
        }
        "memory-shared" => {
            tracing::warn!("You're using shared in-memory remote object store for {}. This should never be used in benchmarks and production environment.", ident);
            ObjectStoreImpl::InMem(InMemObjectStore::shared().monitored(metrics))
        }
        other => {
            unimplemented!(
                "{} remote object store only supports s3, minio, disk, memory, and memory-shared for now.",
                other
            )
        }
    }
}

pub fn parse_local_object_store(url: &str, metrics: Arc<ObjectStoreMetrics>) -> ObjectStoreImpl {
    match url {
        disk if disk.starts_with("disk://") => ObjectStoreImpl::Disk(
            DiskObjectStore::new(disk.strip_prefix("disk://").unwrap()).monitored(metrics),
        ),
        temp_disk if temp_disk.starts_with("tempdisk") => {
            let path = tempfile::TempDir::new()
                .expect("should be able to create temp dir")
                .into_path()
                .to_str()
                .expect("should be able to convert to str")
                .to_owned();
            ObjectStoreImpl::Disk(DiskObjectStore::new(path.as_str()).monitored(metrics))
        }
        "memory" => {
            tracing::warn!("You're using Hummock in-memory local object store. This should never be used in benchmarks and production environment.");
            ObjectStoreImpl::InMem(InMemObjectStore::new().monitored(metrics))
        }
        #[cfg(feature = "hdfs-backend")]
        hdfs if hdfs.starts_with("hdfs://") => {
            let hdfs = hdfs.strip_prefix("hdfs://").unwrap();
            let (namenode, root) = hdfs.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_hdfs_engine(namenode.to_string(), root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }
        gcs if gcs.starts_with("gcs://") => {
            let gcs = gcs.strip_prefix("gcs://").unwrap();
            let (bucket, root) = gcs.split_once('@').unwrap();
            ObjectStoreImpl::Opendal(
                OpendalObjectStore::new_gcs_engine(bucket.to_string(), root.to_string())
                    .unwrap()
                    .monitored(metrics),
            )
        }
        other => {
            unimplemented!(
                "{} Hummock only supports s3, minio, disk, and memory for now.",
                other
            )
        }
    }
}
