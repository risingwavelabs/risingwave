// Copyright 2025 RisingWave Labs
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

use bytes::{Buf, BufMut};

use crate::vector::VectorRef;

#[cfg_attr(test, derive(PartialEq, Debug))]
pub struct VectorBlockInner {
    dimension: u32,
    vector_payload: Vec<f32>,
    info_payload: Vec<u8>,
    info_offset: Vec<u32>,
}

impl VectorBlockInner {
    pub fn count(&self) -> usize {
        self.info_offset.len()
    }

    pub fn vec_ref(&self, idx: usize) -> VectorRef<'_> {
        let start = idx * self.dimension as usize;
        let end = start + self.dimension as usize;
        VectorRef::from_slice(&self.vector_payload[start..end])
    }

    pub fn info(&self, idx: usize) -> &[u8] {
        let start = if idx == 0 {
            0
        } else {
            self.info_offset[idx - 1] as usize
        };
        let end = self.info_offset[idx] as usize;
        &self.info_payload[start..end]
    }
}

pub struct VectorBlockBuilder(VectorBlockInner);

impl VectorBlockBuilder {
    pub fn new(dimension: usize) -> Self {
        Self(VectorBlockInner {
            dimension: dimension
                .try_into()
                .unwrap_or_else(|_| panic!("dimension {} overflow", dimension)),
            vector_payload: vec![],
            info_payload: vec![],
            info_offset: vec![],
        })
    }

    pub fn add(&mut self, vec: VectorRef<'_>, info: &[u8]) {
        let slice = vec.as_slice();
        assert_eq!(self.0.dimension as usize, slice.len());
        self.0.vector_payload.extend_from_slice(slice);
        self.0.info_payload.extend_from_slice(info);
        let offset = self.0.info_payload.len();
        self.0.info_offset.push(
            offset
                .try_into()
                .unwrap_or_else(|_| panic!("offset {} overflow", offset)),
        );
    }

    pub fn finish(self) -> Option<VectorBlock> {
        if !self.0.info_offset.is_empty() {
            Some(VectorBlock(self.0))
        } else {
            None
        }
    }
}

#[cfg_attr(test, derive(PartialEq, Debug))]
pub struct VectorBlock(VectorBlockInner);

impl VectorBlock {
    pub fn count(&self) -> usize {
        self.0.count()
    }

    pub fn vec_ref(&self, idx: usize) -> VectorRef<'_> {
        self.0.vec_ref(idx)
    }

    pub fn info(&self, idx: usize) -> &[u8] {
        self.0.info(idx)
    }

    /// # Format:
    ///
    /// ```plain
    /// | dimension (u32) | vector_count (u32) |
    /// | info_offset (u32) * vector_count |
    /// | info_payload (u8) * (last offset in info_offset) |
    /// | vector_payload (f32) * (dimension * vector_count)  |
    /// ```
    pub fn encode(&self, mut buf: impl BufMut) {
        buf.put_u32(self.0.dimension);
        let vector_count = self.0.info_offset.len();
        assert!(vector_count > 0);
        buf.put_u32(
            vector_count
                .try_into()
                .unwrap_or_else(|_| panic!("vector count {} overflow", vector_count)),
        );
        for offset in &self.0.info_offset {
            buf.put_u32(*offset);
        }
        let last_offset = *self.0.info_offset.last().unwrap();
        assert_eq!(last_offset as usize, self.0.info_payload.len());
        buf.put_slice(&self.0.info_payload);
        assert_eq!(
            self.0.vector_payload.len(),
            (self.0.dimension as usize) * vector_count
        );
        // TODO: put the whole f32 slice as a whole
        for f in &self.0.vector_payload {
            buf.put_f32(*f);
        }
    }

    pub fn decode(mut buf: impl Buf) -> Self {
        let dimension = buf.get_u32();
        let vector_count = buf.get_u32() as usize;
        let mut info_offset = Vec::with_capacity(vector_count);
        for _ in 0..vector_count {
            let offset = buf.get_u32();
            info_offset.push(offset);
        }
        let info_payload_len = *info_offset.last().expect("non-empty") as usize;
        let mut info_payload = vec![0; info_payload_len];
        buf.copy_to_slice(&mut info_payload);
        let vector_item_count = (dimension as usize) * vector_count;
        let mut vector_payload = Vec::with_capacity(vector_item_count);
        for _ in 0..vector_item_count {
            let item = buf.get_f32();
            vector_payload.push(item);
        }
        Self(VectorBlockInner {
            dimension,
            vector_payload,
            info_payload,
            info_offset,
        })
    }
}

impl<'a> IntoIterator for &'a VectorBlock {
    type Item = (VectorRef<'a>, &'a [u8]);

    type IntoIter = impl Iterator<Item = Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        (0..self.0.info_offset.len()).map(|i| {
            let start = if i == 0 {
                0
            } else {
                self.0.info_offset[i - 1] as usize
            };
            let end = self.0.info_offset[i] as usize;
            let info = &self.0.info_payload[start..end];
            let start = i * self.0.dimension as usize;
            let end = start + self.0.dimension as usize;
            (
                VectorRef::from_slice(&self.0.vector_payload[start..end]),
                info,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::hummock::vector::block::{VectorBlock, VectorBlockBuilder};
    use crate::vector::test_utils::{gen_info, gen_vector};

    const VECTOR_DIM: usize = 128;

    #[test]
    fn test_basic() {
        let input = (0..200)
            .map(|i| (gen_vector(VECTOR_DIM), gen_info(i)))
            .collect_vec();
        let mut builder = VectorBlockBuilder::new(VECTOR_DIM);
        for (vec, info) in &input {
            builder.add(vec.to_ref(), info);
        }
        let block = builder.finish().unwrap();
        let mut encoded_block = Vec::new();
        block.encode(&mut encoded_block);
        let decoded_block = VectorBlock::decode(encoded_block.as_slice());
        assert_eq!(block, decoded_block);
    }

    #[test]
    fn test_empty_builder() {
        let builder = VectorBlockBuilder::new(VECTOR_DIM);
        assert!(builder.finish().is_none());
    }
}
