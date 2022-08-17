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

use crate::hummock::{
    CachePolicy, HummockResult, SstableBuilderOptions, SstableStoreRef, SstableStreamingUploader,
};

/// A consumer of SST data.
#[async_trait::async_trait]
pub trait SstableWriter: Send {
    type Output;

    /// Write an SST block to the writer.
    fn write_block(&mut self, block: &[u8]) -> HummockResult<()>;

    /// Finish writing the SST. Return the output along with final data length.
    fn finish(self, size_footer: u32) -> HummockResult<(usize, Self::Output)>;

    /// Get the length of data that has already been written.
    fn data_len(&self) -> usize;
}

/// Append SST data to a buffer.
pub struct InMemSstableWriter {
    buf: BytesMut,
}

/// Append SST data to a buffer.
impl InMemSstableWriter {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
        }
    }
}

#[async_trait::async_trait]
impl SstableWriter for InMemSstableWriter {
    type Output = Bytes;

    fn write_block(&mut self, block: &[u8]) -> HummockResult<()> {
        self.buf.put_slice(block);
        Ok(())
    }

    fn finish(mut self, size_footer: u32) -> HummockResult<(usize, Self::Output)> {
        self.buf.put_slice(&size_footer.to_le_bytes());
        Ok((self.buf.len(), self.buf.freeze()))
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

impl From<&SstableBuilderOptions> for InMemSstableWriter {
    fn from(options: &SstableBuilderOptions) -> InMemSstableWriter {
        InMemSstableWriter::new(options.capacity + options.block_capacity)
    }
}

/// Upload sst blocks to the streaming uploader of `SstableStore`.
/// The upload might not be finished immediately on calling `finish`.
/// Instead, it returns a sealed uploader which is ready to be passed back to `SstableStore` to
/// finish the uploading.
pub struct StreamingSstableWriter {
    uploader: SstableStreamingUploader,
    data_len: usize,
}

impl StreamingSstableWriter {
    pub fn new(uploader: SstableStreamingUploader) -> StreamingSstableWriter {
        Self {
            uploader,
            data_len: 0,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriter for StreamingSstableWriter {
    type Output = SstableStreamingUploader;

    fn write_block(&mut self, block: &[u8]) -> HummockResult<()> {
        self.data_len += block.len();
        self.uploader.upload_block(BytesMut::from(block).freeze())
    }

    fn finish(mut self, size_footer: u32) -> HummockResult<(usize, Self::Output)> {
        self.uploader.upload_size_footer(size_footer)?;
        Ok((self.data_len + 4, self.uploader))
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
    type Writer = InMemSstableWriter;

    async fn build(&self, _sst_id: HummockSstableId) -> HummockResult<Self::Writer> {
        Ok(InMemSstableWriter::new(self.capacity))
    }
}

pub struct StreamingWriterBuilder {
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
}

impl StreamingWriterBuilder {
    pub fn new(sstable_store: SstableStoreRef, policy: CachePolicy) -> StreamingWriterBuilder {
        Self {
            sstable_store,
            policy,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriterBuilder for StreamingWriterBuilder {
    type Writer = StreamingSstableWriter;

    async fn build(&self, sst_id: HummockSstableId) -> HummockResult<Self::Writer> {
        let uploader = self
            .sstable_store
            .put_sst_stream(sst_id, self.policy)
            .await?;
        Ok(StreamingSstableWriter::new(uploader))
    }
}
