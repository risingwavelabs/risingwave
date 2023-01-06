
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use tokio::io::AsyncRead;
use tokio::sync::Mutex;

use super::{
    BlockLocation, BoxedStreamingUploader, ObjectError, ObjectMetadata, ObjectResult, ObjectStore,
    StreamingUploader,
};
/// In-memory object storage, useful for testing.
#[derive(Default, Clone)]
pub struct HdfsObjectStore {
    objects: Arc<Mutex<HashMap<String, (ObjectMetadata, Bytes)>>>,
}

#[async_trait::async_trait]
impl ObjectStore for HdfsObjectStore {
    fn get_object_prefix(&self, _obj_id: u64) -> String {
        unimplemented!()
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        unimplemented!()
    }

    fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader> {
        unimplemented!()
    }

    async fn read(&self, path: &str, block: Option<BlockLocation>) -> ObjectResult<Bytes> {
        unimplemented!()
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        unimplemented!()
    }

    /// Returns a stream reading the object specified in `path`. If given, the stream starts at the
    /// byte with index `start_pos` (0-based). As far as possible, the stream only loads the amount
    /// of data into memory that is read from the stream.
    async fn streaming_read(
        &self,
        path: &str,
        start_pos: Option<usize>,
    ) -> ObjectResult<Box<dyn AsyncRead + Unpin + Send + Sync>> {
        unimplemented!()
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        unimplemented!()
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        unimplemented!()
    }

    /// Deletes the objects with the given paths permanently from the storage. If an object
    /// specified in the request is not found, it will be considered as successfully deleted.
    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        unimplemented!()
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        unimplemented!()
    }

    fn store_media_type(&self) -> &'static str {
        "hdfs"
    }
}