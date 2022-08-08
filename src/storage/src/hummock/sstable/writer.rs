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

use crate::hummock::{HummockResult, SstableBuilderOptions};

/// A consumer of SST data.
/// The data may be written to a stream, pushed to a vector, appended to a buffer, etc.
#[async_trait::async_trait]
pub trait SstableWriter: Send {
    type Output;

    async fn write(&mut self, data: &[u8]) -> HummockResult<()>;
    async fn finish(self) -> HummockResult<Self::Output>;
    fn data_len(&self) -> usize;
}

pub struct InMemSstableWriter {
    buf: BytesMut,
}

/// Append sst data to a buffer.
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

    async fn write(&mut self, data: &[u8]) -> HummockResult<()> {
        self.buf.put_slice(data);
        Ok(())
    }

    async fn finish(self) -> HummockResult<Bytes> {
        Ok(self.buf.freeze())
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

#[async_trait::async_trait]
pub trait SstableWriterBuilder: Send + Sync {
    type Writer: SstableWriter;

    async fn build(&self) -> HummockResult<Self::Writer>;
}

pub struct InMemWriterBuilder {
    capacity: usize,
}

impl From<&SstableBuilderOptions> for InMemWriterBuilder {
    fn from(opt: &SstableBuilderOptions) -> InMemWriterBuilder {
        InMemWriterBuilder {
            capacity: get_in_mem_writer_capacity_from_opt(opt),
        }
    }
}

#[async_trait::async_trait]
impl SstableWriterBuilder for InMemWriterBuilder {
    type Writer = InMemSstableWriter;

    async fn build(&self) -> HummockResult<Self::Writer> {
        Ok(InMemSstableWriter::new(self.capacity))
    }
}

fn get_in_mem_writer_capacity_from_opt(opt: &SstableBuilderOptions) -> usize {
    opt.capacity + opt.block_capacity
}
