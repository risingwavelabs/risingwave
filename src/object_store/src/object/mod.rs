// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;

pub mod mem;
pub use mem::*;

pub mod s3;
pub use s3::*;

mod disk;
pub mod error;
pub mod object_metrics;

pub use error::*;
use object_metrics::ObjectStoreMetrics;

use crate::object::disk::DiskObjectStore;

pub const LOCAL_OBJECT_STORE_PATH_PREFIX: &str = "@local:";

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

#[derive(Debug, Copy, Clone)]
pub struct BlockLocation {
    pub offset: usize,
    pub size: usize,
}

pub struct ObjectMetadata {
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

pub trait EmptyFutureTrait<'a> = Future<Output = ObjectResult<()>> + 'a;
pub trait ReadFutureTrait<'a> = Future<Output = ObjectResult<Bytes>> + 'a;
pub trait ReadvFutureTrait<'a> = Future<Output = ObjectResult<Vec<Bytes>>> + 'a;
pub trait MetadataFutureTrait<'a> = Future<Output = ObjectResult<ObjectMetadata>> + 'a;

#[macro_export]
macro_rules! define_object_store_associated_types {
    () => {
        type UploadFuture<'a> = impl $crate::object::EmptyFutureTrait<'a>;
        type ReadFuture<'a> = impl $crate::object::ReadFutureTrait<'a>;
        type ReadvFuture<'a> = impl $crate::object::ReadvFutureTrait<'a>;
        type MetadataFuture<'a> = impl $crate::object::MetadataFutureTrait<'a>;
        type DeleteFuture<'a> = impl $crate::object::EmptyFutureTrait<'a>;
    };
}

/// The implementation must be thread-safe.
pub trait ObjectStore: Send + Sync {
    type UploadFuture<'a>: EmptyFutureTrait<'a>
    where
        Self: 'a;
    type ReadFuture<'a>: ReadFutureTrait<'a>
    where
        Self: 'a;
    type ReadvFuture<'a>: ReadvFutureTrait<'a>
    where
        Self: 'a;
    type MetadataFuture<'a>: MetadataFutureTrait<'a>
    where
        Self: 'a;
    type DeleteFuture<'a>: EmptyFutureTrait<'a>
    where
        Self: 'a;

    /// Uploads the object to `ObjectStore`.
    fn upload<'a>(&'a self, path: &'a str, obj: Bytes) -> Self::UploadFuture<'_>;

    /// If the `block_loc` is None, the whole object will be return.
    /// If objects are PUT using a multipart upload, it’s a good practice to GET them in the same
    /// part sizes (or at least aligned to part boundaries) for best performance.
    /// <https://d1.awsstatic.com/whitepapers/AmazonS3BestPractices.pdf?stod_obj2>
    fn read<'a>(&'a self, path: &'a str, block_loc: Option<BlockLocation>) -> Self::ReadFuture<'_>;

    fn readv<'a>(&'a self, path: &'a str, block_locs: &'a [BlockLocation])
        -> Self::ReadvFuture<'_>;

    /// Obtains the object metadata.
    fn metadata<'a>(&'a self, path: &'a str) -> Self::MetadataFuture<'_>;

    /// Deletes blob permanently.
    fn delete<'a>(&'a self, path: &'a str) -> Self::DeleteFuture<'_>;

    fn monitored(self, metrics: Arc<ObjectStoreMetrics>) -> MonitoredObjectStore<Self>
    where
        Self: Sized,
    {
        MonitoredObjectStore::new(self, metrics)
    }
}

/// Macro to generate code for object store implementations.
/// `$macro` should take input argument in the format of
/// `($object_store:expr, $method_name:ident, $path:expr $(, $args:expr)*)`.
macro_rules! object_store_impl_body {
    ($name:ident, $macro:ident) => {
        impl $name {
            pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
                $macro!(self, upload, path, obj)
            }

            pub async fn read(
                &self,
                path: &str,
                block_loc: Option<BlockLocation>,
            ) -> ObjectResult<Bytes> {
                $macro!(self, read, path, block_loc)
            }

            pub async fn readv(
                &self,
                path: &str,
                block_locs: &[BlockLocation],
            ) -> ObjectResult<Vec<Bytes>> {
                $macro!(self, readv, path, block_locs)
            }

            pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
                $macro!(self, metadata, path)
            }

            pub async fn delete(&self, path: &str) -> ObjectResult<()> {
                $macro!(self, delete, path)
            }
        }
    };
}

pub type ObjectStoreRef = Arc<ObjectStoreImpl>;

pub enum LocalObjectStore {
    InMem(MonitoredObjectStore<InMemObjectStore>),
    Disk(MonitoredObjectStore<DiskObjectStore>),
}

macro_rules! local_object_store_method_body {
    ($object_store:expr, $method_name:ident, $path:expr $(, $args:expr)*) => {
        {
            let orig_path = $path;
            let path = parse_object_store_path($path);
            assert!(path.is_local(), "local object store only but the path is not local: {}", orig_path);
            match $object_store {
                LocalObjectStore::InMem(in_mem) => in_mem.$method_name(path.as_str() $(, $args)*).await,
                LocalObjectStore::Disk(disk) => disk.$method_name(path.as_str() $(, $args)*).await,
            }
        }
    };
}

object_store_impl_body!(LocalObjectStore, local_object_store_method_body);

pub enum RemoteObjectStore {
    InMem(MonitoredObjectStore<InMemObjectStore>),
    Disk(MonitoredObjectStore<DiskObjectStore>),
    S3(MonitoredObjectStore<S3ObjectStore>),
}

macro_rules! remote_object_store_method_body {
    ($object_store:expr, $method_name:ident, $path:expr $(, $args:expr)*) => {
        {
            let orig_path = $path;
            let path = parse_object_store_path($path);
            assert!(path.is_remote(), "remote object store only but the path is not remote: {}", orig_path);
            match $object_store {
                RemoteObjectStore::InMem(in_mem) => in_mem.$method_name(path.as_str() $(, $args)*).await,
                RemoteObjectStore::Disk(disk) => disk.$method_name(path.as_str() $(, $args)*).await,
                RemoteObjectStore::S3(s3) => s3.$method_name(path.as_str() $(, $args)*).await,
            }
        }
    };
}

object_store_impl_body!(RemoteObjectStore, remote_object_store_method_body);

pub enum ObjectStoreImpl {
    Local(LocalObjectStore),
    Remote(RemoteObjectStore),
    Hybrid {
        local: LocalObjectStore,
        remote: RemoteObjectStore,
    },
}

impl ObjectStoreImpl {
    pub fn hybrid(local: LocalObjectStore, remote: RemoteObjectStore) -> Self {
        ObjectStoreImpl::Hybrid { local, remote }
    }
}

/// For `path`, if the `path` starts with `LOCAL_OBJECT_STORE_PATH_PREFIX`, it indicates that the
/// operation should be performed on the local object store, and otherwise the operation should be
/// performed on remote object store.
macro_rules! object_store_impl_method_body {
    ($object_store:expr, $method_name:ident, $path:expr $(, $args:expr)*) => {
        {
            let orig_path = $path;
            let path = parse_object_store_path($path);
            match $object_store {
                ObjectStoreImpl::Local(local) => local.$method_name(orig_path $(, $args)*).await,
                ObjectStoreImpl::Remote(remote) => remote.$method_name(orig_path $(, $args)*).await,
                ObjectStoreImpl::Hybrid {
                    local: local,
                    remote: remote,
                } => {
                    match path {
                        ObjectStorePath::Local(_path) => local.$method_name(orig_path $(, $args)*).await,
                        ObjectStorePath::Remote(_path) => remote.$method_name(orig_path $(, $args)*).await,
                    }
                }
            }
        }
    };
}

object_store_impl_body!(ObjectStoreImpl, object_store_impl_method_body);

pub struct MonitoredObjectStore<OS: ObjectStore> {
    inner: OS,
    object_store_metrics: Arc<ObjectStoreMetrics>,
}

/// Manually dispatch trait methods.
impl<OS: ObjectStore> MonitoredObjectStore<OS> {
    pub fn new(store: OS, object_store_metrics: Arc<ObjectStoreMetrics>) -> Self {
        Self {
            inner: store,
            object_store_metrics,
        }
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        self.object_store_metrics
            .write_bytes
            .inc_by(obj.len() as u64);
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["upload"])
            .start_timer();
        self.object_store_metrics
            .operation_size
            .with_label_values(&["upload"])
            .observe(obj.len() as f64);
        self.inner.upload(path, obj).await?;
        Ok(())
    }

    pub async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["read"])
            .start_timer();
        let ret = self.inner.read(path, block_loc).await?;
        self.object_store_metrics
            .read_bytes
            .inc_by(ret.len() as u64);
        self.object_store_metrics
            .operation_size
            .with_label_values(&["read"])
            .observe(ret.len() as f64);
        Ok(ret)
    }

    pub async fn readv(
        &self,
        path: &str,
        block_locs: &[BlockLocation],
    ) -> ObjectResult<Vec<Bytes>> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["readv"])
            .start_timer();
        let ret = self.inner.readv(path, block_locs).await?;
        self.object_store_metrics
            .read_bytes
            .inc_by(ret.iter().map(|block| block.len()).sum::<usize>() as u64);
        Ok(ret)
    }

    pub async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["metadata"])
            .start_timer();
        self.inner.metadata(path).await
    }

    pub async fn delete(&self, path: &str) -> ObjectResult<()> {
        let _timer = self
            .object_store_metrics
            .operation_latency
            .with_label_values(&["delete"])
            .start_timer();
        self.inner.delete(path).await
    }
}

pub async fn parse_remote_object_store(
    url: &str,
    metrics: Arc<ObjectStoreMetrics>,
) -> RemoteObjectStore {
    match url {
        s3 if s3.starts_with("s3://") => RemoteObjectStore::S3(
            S3ObjectStore::new(s3.strip_prefix("s3://").unwrap().to_string())
                .await
                .monitored(metrics),
        ),
        minio if minio.starts_with("minio://") => {
            RemoteObjectStore::S3(S3ObjectStore::with_minio(minio).await.monitored(metrics))
        }
        disk if disk.starts_with("disk://") => RemoteObjectStore::Disk(
            DiskObjectStore::new(disk.strip_prefix("disk://").unwrap()).monitored(metrics),
        ),
        memory if memory.starts_with("memory") => {
            tracing::warn!("You're using Hummock in-memory remote object store. This should never be used in benchmarks and production environment.");
            RemoteObjectStore::InMem(InMemObjectStore::new().monitored(metrics))
        }
        other => {
            unimplemented!(
                "{} hummock remote object store only supports s3, minio, disk, and memory for now.",
                other
            )
        }
    }
}

pub async fn parse_local_object_store(
    url: &str,
    metrics: Arc<ObjectStoreMetrics>,
) -> LocalObjectStore {
    match url {
        disk if disk.starts_with("disk://") => LocalObjectStore::Disk(
            DiskObjectStore::new(disk.strip_prefix("disk://").unwrap()).monitored(metrics),
        ),
        temp_disk if temp_disk.starts_with("tempdisk") => {
            let path = tempfile::TempDir::new()
                .expect("should be able to create temp dir")
                .into_path()
                .to_str()
                .expect("should be able to convert to str")
                .to_owned();
            LocalObjectStore::Disk(DiskObjectStore::new(path.as_str()).monitored(metrics))
        }
        memory if memory.starts_with("memory") => {
            tracing::warn!("You're using Hummock in-memory local object store. This should never be used in benchmarks and production environment.");
            LocalObjectStore::InMem(InMemObjectStore::new().monitored(metrics))
        }
        other => {
            unimplemented!(
                "{} Hummock only supports s3, minio, disk, and  memory for now.",
                other
            )
        }
    }
}
