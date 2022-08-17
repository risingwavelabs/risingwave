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

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::HummockSstableId;
use tokio::task::JoinHandle;

use super::SstableMeta;
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    CachePolicy, HummockResult, SstableBuilderOptions, SstableStoreRef, SstableStreamingUploader,
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

/// Append SST data to a buffer.
pub struct InMemWriter {
    buf: BytesMut,
}

/// Append SST data to a buffer. Used for tests and benchmarks.
impl InMemWriter {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
        }
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
        _tracker: Option<MemoryTracker>,
    ) -> HummockResult<Self::Output> {
        self.buf.put_slice(&get_size_footer(&meta).to_le_bytes());
        Ok((self.buf.freeze(), meta))
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

impl From<&SstableBuilderOptions> for InMemWriter {
    fn from(options: &SstableBuilderOptions) -> InMemWriter {
        InMemWriter::new(options.capacity + options.block_capacity)
    }
}

/// Upload sst data to `SstableStore` as a whole on calling `finish`.
/// The upload is finished when the returned `JoinHandle` is joined.
pub struct BatchUploadWriter {
    sstable_store: SstableStoreRef,
    sst_id: HummockSstableId,
    policy: CachePolicy,
    buf: BytesMut,
}

impl BatchUploadWriter {
    pub fn new(
        sstable_store: SstableStoreRef,
        sst_id: HummockSstableId,
        policy: CachePolicy,
        capacity: usize,
    ) -> Self {
        Self {
            sstable_store,
            sst_id,
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
            let ret = self
                .sstable_store
                .put_sst(self.sst_id, meta, data, self.policy)
                .await;
            drop(tracker);
            ret
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
    sstable_store: SstableStoreRef,
    uploader: SstableStreamingUploader,
    data_len: usize,
}

impl StreamingUploadWriter {
    pub fn new(sstable_store: SstableStoreRef, uploader: SstableStreamingUploader) -> Self {
        Self {
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
            let ret = self
                .sstable_store
                .finish_put_sst_stream(self.uploader, meta)
                .await;
            drop(tracker);
            ret
        });
        Ok(join_handle)
    }

    fn data_len(&self) -> usize {
        self.data_len
    }
}

#[async_trait::async_trait]
pub trait SstableWriterBuilder: Send + Sync {
    type Writer: SstableWriter;

    async fn build(&self, sst_id: HummockSstableId) -> HummockResult<Self::Writer>;
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

#[async_trait::async_trait]
impl SstableWriterBuilder for InMemWriterBuilder {
    type Writer = InMemWriter;

    async fn build(&self, _sst_id: HummockSstableId) -> HummockResult<Self::Writer> {
        Ok(InMemWriter::new(self.capacity))
    }
}

pub struct BatchUploadWriterBuilder {
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
    capacity: usize,
}

impl BatchUploadWriterBuilder {
    pub fn new(
        opt: &SstableBuilderOptions,
        sstable_store: SstableStoreRef,
        policy: CachePolicy,
    ) -> BatchUploadWriterBuilder {
        BatchUploadWriterBuilder {
            sstable_store,
            policy,
            capacity: opt.capacity + opt.block_capacity,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriterBuilder for BatchUploadWriterBuilder {
    type Writer = BatchUploadWriter;

    async fn build(&self, sst_id: HummockSstableId) -> HummockResult<Self::Writer> {
        Ok(BatchUploadWriter::new(
            self.sstable_store.clone(),
            sst_id,
            self.policy,
            self.capacity,
        ))
    }
}

pub struct StreamingUploadWriterBuilder {
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
}

impl StreamingUploadWriterBuilder {
    pub fn new(
        sstable_store: SstableStoreRef,
        policy: CachePolicy,
    ) -> StreamingUploadWriterBuilder {
        Self {
            sstable_store,
            policy,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriterBuilder for StreamingUploadWriterBuilder {
    type Writer = StreamingUploadWriter;

    async fn build(&self, sst_id: HummockSstableId) -> HummockResult<Self::Writer> {
        let uploader = self
            .sstable_store
            .put_sst_stream(sst_id, self.policy)
            .await?;
        Ok(StreamingUploadWriter::new(
            self.sstable_store.clone(),
            uploader,
        ))
    }
}

fn get_size_footer(meta: &SstableMeta) -> u32 {
    meta.block_metas.len() as u32
}
