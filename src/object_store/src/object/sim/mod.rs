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

mod client;
mod error;
use bytes::{BufMut, BytesMut};
pub use error::SimError;
use futures::Stream;
mod rpc_server;
mod service;

use std::net::SocketAddr;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use risingwave_common::range::RangeBoundsExt;

use self::client::Client;
use self::service::Response;
use super::{
    BoxedStreamingUploader, Bytes, ObjectDataStream, ObjectError, ObjectMetadata,
    ObjectMetadataIter, ObjectRangeBounds, ObjectResult, ObjectStore, StreamingUploader,
};

type PartId = i32;

const MIN_PART_ID: PartId = 1;

const STREAM_PART_SIZE: usize = 16 * 1024 * 1024;

pub struct SimStreamingUploader {
    client: Client,
    part_size: usize,
    key: String,
    buf: BytesMut,
    not_uploaded_len: usize,
}

impl SimStreamingUploader {
    fn new(client: Client, key: String, part_size: usize) -> Self {
        Self {
            client,
            key,
            part_size,
            buf: Default::default(),
            not_uploaded_len: 0,
        }
    }
}

#[async_trait::async_trait]
impl StreamingUploader for SimStreamingUploader {
    async fn write_bytes(&mut self, data: Bytes) -> ObjectResult<()> {
        let data_len = data.len();
        self.buf.put(data);

        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> ObjectResult<()> {
        if self.buf.is_empty() {
            debug_assert_eq!(self.not_uploaded_len, 0);
            Err(ObjectError::internal("upload empty object"))
        } else {
            let resp = self
                .client
                .send_request(service::Request::Upload {
                    path: self.key,
                    obj: self.buf.freeze(),
                })
                .await?;
            let Response::Upload = resp else {
                return Err(SimError::other("expect Response::Upload").into());
            };
            Ok(())
        }
    }

    fn get_memory_usage(&self) -> u64 {
        self.buf.len() as u64
    }
}

pub struct SimDataIterator {
    data: Bytes,
    offset: usize,
}

impl SimDataIterator {
    pub fn new(data: Bytes) -> Self {
        Self { data, offset: 0 }
    }
}

impl Stream for SimDataIterator {
    type Item = ObjectResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const MAX_PACKET_SIZE: usize = 128 * 1024;
        if self.offset >= self.data.len() {
            return Poll::Ready(None);
        }
        let read_len = std::cmp::min(self.data.len() - self.offset, MAX_PACKET_SIZE);
        let data = self.data.slice(self.offset..(self.offset + read_len));
        self.offset += read_len;
        Poll::Ready(Some(Ok(data)))
    }
}

pub struct SimObjectStore {
    client: Client,
    part_size: usize,
}

#[async_trait::async_trait]
impl ObjectStore for SimObjectStore {
    fn get_object_prefix(&self, _obj_id: u64) -> String {
        String::default()
    }

    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        if obj.is_empty() {
            Err(ObjectError::internal("upload empty object"))
        } else {
            let path = path.to_string();
            let resp = self
                .client
                .send_request(service::Request::Upload { path, obj })
                .await?;
            if let Response::Upload = resp {
                Ok(())
            } else {
                Err(SimError::other("expect Response::Upload").into())
            }
        }
    }

    async fn streaming_upload(&self, path: &str) -> ObjectResult<BoxedStreamingUploader> {
        Ok(Box::new(SimStreamingUploader::new(
            self.client.clone(),
            path.to_string(),
            self.part_size,
        )))
    }

    async fn read(&self, path: &str, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::Read { path })
            .await?;
        if let Response::Read(obj) = resp {
            if let Some(end) = range.end()
                && end > obj.len()
            {
                return Err(SimError::other("bad block offset and size").into());
            }
            Ok(obj.slice(range))
        } else {
            Err(SimError::other("expect Response::Read").into())
        }
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let path = path.to_string();
        if let Response::Metadata(m) = self
            .client
            .send_request(service::Request::Metadata { path })
            .await?
        {
            Ok(m)
        } else {
            Err(SimError::other("expect Response::Metadata").into())
        }
    }

    async fn streaming_read(
        &self,
        path: &str,
        range: Range<usize>,
    ) -> ObjectResult<ObjectDataStream> {
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::Read { path })
            .await?;
        let Response::Read(body) = resp else {
            return Err(SimError::other("expect Response::Read").into());
        };

        Ok(Box::pin(SimDataIterator::new(body)))
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::Delete { path })
            .await?;
        if let Response::Delete = resp {
            Ok(())
        } else {
            Err(SimError::other("expect Response::Delete").into())
        }
    }

    async fn delete_objects(&self, paths: &[String]) -> ObjectResult<()> {
        let mut error = None;
        for path in paths {
            error = error.or(self.delete(path).await.err());
        }
        if let Some(e) = error {
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn list(&self, path: &str) -> ObjectResult<ObjectMetadataIter> {
        let path = path.to_string();
        let resp = self
            .client
            .send_request(service::Request::List { path })
            .await?;
        if let Response::List(o) = resp {
            Ok(Box::pin(o))
        } else {
            Err(SimError::other("expect Response::List").into())
        }
    }

    fn store_media_type(&self) -> &'static str {
        "sim"
    }
}

impl SimObjectStore {
    pub fn new(addr: &str) -> Self {
        Self {
            client: Client::new(addr.parse::<SocketAddr>().expect("parse SockAddr failed")),
            part_size: STREAM_PART_SIZE,
        }
    }
}
