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

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::Future;
use risingwave_hummock_sdk::HummockSstableId;
use tokio::task::JoinHandle;
use zstd::zstd_safe::WriteBuf;

use super::SstableMeta;
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    CachePolicy, HummockResult, SstableBuilderOptions, SstableStoreWrite, SstableStreamingUploader,
};

/// A consumer of SST data.
pub trait SstableWriter: Send {
    type Output;

    /// Write an SST block to the writer.
    fn write_block(&mut self, block: &[u8]) -> HummockResult<()>;

    /// Finish writing the SST.
    fn finish(
        self,
        meta: SstableMeta,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<Self::Output>;

    /// Get the length of data that has already been written.
    fn data_len(&self) -> usize;
}

/// Append SST data to a buffer. Used for tests and benchmarks.
pub struct InMemWriter {
    sst_id: HummockSstableId,
    buf: BytesMut,
}

impl InMemWriter {
    pub fn new(sst_id: HummockSstableId, capacity: usize) -> Self {
        Self {
            sst_id,
            buf: BytesMut::with_capacity(capacity),
        }
    }

    pub fn new_from_options(sst_id: HummockSstableId, options: &SstableBuilderOptions) -> Self {
        Self::new(sst_id, options.capacity + options.block_capacity)
    }
}

impl SstableWriter for InMemWriter {
    type Output = (Bytes, SstableMeta);

    fn write_block(&mut self, block: &[u8]) -> HummockResult<()> {
        self.buf.put_slice(block);
        Ok(())
    }

    fn finish(
        mut self,
        meta: SstableMeta,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<Self::Output> {
        self.buf.put_slice(&get_size_footer(&meta).to_le_bytes());
        let data = self.buf.freeze();
        if let Some(mut tracker) = tracker {
            if !tracker.try_increase_memory(data.capacity() as u64 + meta.encoded_size() as u64) {
                tracing::debug!("failed to allocate increase memory for meta file, sst id: {}, file size: {}, meta size: {}",
                            self.sst_id, data.capacity(), meta.encoded_size());
            }
        }
        Ok((data, meta))
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

/// Upload sst data to `SstableStore` as a whole on calling `finish`.
/// The upload is finished when the returned `JoinHandle` is joined.
pub struct BatchUploadWriter {
    sst_id: HummockSstableId,
    sstable_store: Arc<dyn SstableStoreWrite>,
    policy: CachePolicy,
    buf: BytesMut,
}

impl BatchUploadWriter {
    pub fn new(
        sst_id: HummockSstableId,
        sstable_store: Arc<dyn SstableStoreWrite>,
        policy: CachePolicy,
        capacity: usize,
    ) -> Self {
        Self {
            sst_id,
            sstable_store,
            policy,
            buf: BytesMut::with_capacity(capacity),
        }
    }
}

impl SstableWriter for BatchUploadWriter {
    type Output = JoinHandle<HummockResult<()>>;

    fn write_block(&mut self, block: &[u8]) -> HummockResult<()> {
        self.buf.put_slice(block);
        Ok(())
    }

    fn finish(
        mut self,
        meta: SstableMeta,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<Self::Output> {
        self.buf.put_slice(&get_size_footer(&meta).to_le_bytes());
        let data = self.buf.freeze();
        let join_handle = tokio::spawn(async move {
            if let Some(mut tracker) = tracker {
                if !tracker.try_increase_memory(data.capacity() as u64 + meta.encoded_size() as u64)
                {
                    tracing::debug!("failed to allocate increase memory for meta file, sst id: {}, file size: {}, meta size: {}",
                                self.sst_id, data.capacity(), meta.encoded_size());
                }
                let ret = self
                    .sstable_store
                    .put_sst(self.sst_id, meta, data, self.policy)
                    .await;
                drop(tracker);
                ret
            } else {
                Ok(())
            }
        });
        Ok(join_handle)
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

/// Upload sst blocks to the streaming uploader of `SstableStore`.
/// The upload is finished when the returned `JoinHandle` is joined.
pub struct StreamingUploadWriter {
    sst_id: HummockSstableId,
    sstable_store: Arc<dyn SstableStoreWrite>,
    uploader: SstableStreamingUploader,
    data_len: usize,
}

impl StreamingUploadWriter {
    pub fn new(
        sst_id: HummockSstableId,
        sstable_store: Arc<dyn SstableStoreWrite>,
        uploader: SstableStreamingUploader,
    ) -> Self {
        Self {
            sst_id,
            sstable_store,
            uploader,
            data_len: 0,
        }
    }
}

impl SstableWriter for StreamingUploadWriter {
    type Output = JoinHandle<HummockResult<()>>;

    fn write_block(&mut self, block: &[u8]) -> HummockResult<()> {
        self.data_len += block.len();
        self.uploader.upload_block(BytesMut::from(block).freeze())
    }

    fn finish(
        mut self,
        meta: SstableMeta,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<Self::Output> {
        self.uploader.upload_size_footer(get_size_footer(&meta))?;
        let join_handle = tokio::spawn(async move {
            let uploader_memory_usage = self.uploader.get_memory_usage();
            if let Some(mut tracker) = tracker {
                if !tracker
                    .try_increase_memory(uploader_memory_usage as u64 + meta.encoded_size() as u64)
                {
                    tracing::debug!("failed to allocate increase memory for meta file, sst id: {}, data size: {}, meta size: {}",
                                self.sst_id, uploader_memory_usage, meta.encoded_size());
                }
                let ret = self
                    .sstable_store
                    .finish_put_sst_stream(self.uploader, meta)
                    .await;
                drop(tracker);
                ret
            } else {
                Ok(())
            }
        });
        Ok(join_handle)
    }

    fn data_len(&self) -> usize {
        self.data_len
    }
}

pub trait SstableWriterBuilder: Send + Sync + 'static {
    type Writer: SstableWriter;
    type BuildFuture<'a>: Future<Output = HummockResult<Self::Writer>> + Send + 'a
    where
        Self: 'a;

    fn build(&self, sst_id: HummockSstableId) -> Self::BuildFuture<'_>;
}

pub struct InMemWriterBuilder {
    capacity: usize,
}

impl From<&SstableBuilderOptions> for InMemWriterBuilder {
    fn from(opt: &SstableBuilderOptions) -> InMemWriterBuilder {
        InMemWriterBuilder {
            capacity: opt.capacity + opt.block_capacity,
        }
    }
}

impl SstableWriterBuilder for InMemWriterBuilder {
    type Writer = InMemWriter;

    type BuildFuture<'a> = impl Future<Output = HummockResult<Self::Writer>> + 'a;

    fn build(&self, sst_id: HummockSstableId) -> Self::BuildFuture<'_> {
        async move { Ok(InMemWriter::new(sst_id, self.capacity)) }
    }
}

pub struct BatchUploadWriterBuilder {
    sstable_store: Arc<dyn SstableStoreWrite>,
    policy: CachePolicy,
    capacity: usize,
}

impl BatchUploadWriterBuilder {
    pub fn new(
        opt: &SstableBuilderOptions,
        sstable_store: Arc<dyn SstableStoreWrite>,
        policy: CachePolicy,
    ) -> BatchUploadWriterBuilder {
        BatchUploadWriterBuilder {
            sstable_store,
            policy,
            capacity: opt.capacity + opt.block_capacity,
        }
    }
}

impl SstableWriterBuilder for BatchUploadWriterBuilder {
    type Writer = BatchUploadWriter;

    type BuildFuture<'a> = impl Future<Output = HummockResult<Self::Writer>> + 'a;

    fn build(&self, sst_id: HummockSstableId) -> Self::BuildFuture<'_> {
        async move {
            Ok(BatchUploadWriter::new(
                sst_id,
                self.sstable_store.clone(),
                self.policy,
                self.capacity,
            ))
        }
    }
}

pub struct StreamingUploadWriterBuilder {
    sstable_store: Arc<dyn SstableStoreWrite>,
    policy: CachePolicy,
}

impl StreamingUploadWriterBuilder {
    pub fn new(
        sstable_store: Arc<dyn SstableStoreWrite>,
        policy: CachePolicy,
    ) -> StreamingUploadWriterBuilder {
        Self {
            sstable_store,
            policy,
        }
    }
}

impl SstableWriterBuilder for StreamingUploadWriterBuilder {
    type Writer = StreamingUploadWriter;

    type BuildFuture<'a> = impl Future<Output = HummockResult<Self::Writer>> + 'a;

    fn build(&self, sst_id: HummockSstableId) -> Self::BuildFuture<'_> {
        async move {
            let uploader = self
                .sstable_store
                .create_put_sst_stream(sst_id, self.policy)
                .await?;
            Ok(StreamingUploadWriter::new(
                sst_id,
                self.sstable_store.clone(),
                uploader,
            ))
        }
    }
}

fn get_size_footer(meta: &SstableMeta) -> u32 {
    meta.block_metas.len() as u32
}
