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
pub trait SstableWriter: Send {
    type Output;

    /// Write an SST block to the writer.
    fn write_block(&mut self, block: &[u8]) -> HummockResult<()>;

    /// Finish writing the SST.
    fn finish(self: Box<Self>, size_footer: u32) -> HummockResult<Self::Output>;

    /// Get the length of data that has already been written.
    fn data_len(&self) -> usize;
}

/// Append SST data to a buffer. Used for tests and benchmarks.
pub struct InMemWriter {
    buf: BytesMut,
}

impl InMemWriter {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
        }
    }
}

impl From<&SstableBuilderOptions> for InMemWriter {
    fn from(options: &SstableBuilderOptions) -> Self {
        Self::new(options.capacity + options.block_capacity)
    }
}

impl SstableWriter for InMemWriter {
    type Output = Bytes;

    fn write_block(&mut self, block: &[u8]) -> HummockResult<()> {
        self.buf.put_slice(block);
        Ok(())
    }

    fn finish(mut self: Box<Self>, size_footer: u32) -> HummockResult<Self::Output> {
        self.buf.put_slice(&size_footer.to_le_bytes());
        let data = self.buf.freeze();
        Ok(data)
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rand::{Rng, SeedableRng};

    use crate::hummock::sstable::VERSION;
    use crate::hummock::{BlockMeta, InMemWriter, SstableMeta, SstableWriter};

    fn get_sst() -> (Bytes, Vec<Bytes>, SstableMeta) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let mut buffer: Vec<u8> = vec![0; 5000];
        rng.fill(&mut buffer[..]);
        buffer.extend((5_u32).to_le_bytes());
        let data = Bytes::from(buffer);

        let mut blocks = Vec::with_capacity(5);
        let mut block_metas = Vec::with_capacity(5);
        for i in 0..5 {
            block_metas.push(BlockMeta {
                smallest_key: Vec::new(),
                len: 1000,
                offset: i * 1000,
            });
            blocks.push(data.slice((i * 1000) as usize..((i + 1) * 1000) as usize));
        }
        let meta = SstableMeta {
            block_metas,
            bloom_filter: Vec::new(),
            estimated_size: 0,
            key_count: 0,
            smallest_key: Vec::new(),
            largest_key: Vec::new(),
            version: VERSION,
        };

        (data, blocks, meta)
    }

    #[test]
    fn test_in_mem_writer() {
        let (data, blocks, _) = get_sst();
        let mut writer = Box::new(InMemWriter::new(0));
        blocks.iter().for_each(|b| {
            writer.write_block(&b[..]).unwrap();
        });
        let output_data = writer.finish(blocks.len() as u32).unwrap();
        assert_eq!(output_data, data);
    }
}
