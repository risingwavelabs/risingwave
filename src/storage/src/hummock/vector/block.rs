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

use std::slice;

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

    pub fn encoded_len(&self) -> usize {
        size_of::<u32>() // dimension
            + size_of::<u32>() // vector count
            + self.info_offset.len() * size_of::<u32>() // info offsets
            + self.info_payload.len() // info payload
            + self.vector_payload.len() * size_of::<f32>() // vector payload
    }
}

pub struct VectorBlockBuilder {
    inner: VectorBlockInner,
    encoded_len: usize,
}

impl VectorBlockBuilder {
    pub fn new(dimension: usize) -> Self {
        Self {
            inner: VectorBlockInner {
                dimension: dimension
                    .try_into()
                    .unwrap_or_else(|_| panic!("dimension {} overflow", dimension)),
                vector_payload: vec![],
                info_payload: vec![],
                info_offset: vec![],
            },
            encoded_len: size_of::<u32>() // dimension
             + size_of::<u32>(), // vector count
        }
    }

    pub fn add(&mut self, vec: VectorRef<'_>, info: &[u8]) {
        let slice = vec.as_slice();
        assert_eq!(self.inner.dimension as usize, slice.len());
        self.inner.vector_payload.extend_from_slice(slice);
        self.inner.info_payload.extend_from_slice(info);
        let offset = self.inner.info_payload.len();
        self.inner.info_offset.push(
            offset
                .try_into()
                .unwrap_or_else(|_| panic!("offset {} overflow", offset)),
        );
        self.encoded_len += size_of::<u32>() + size_of_val(slice) + info.len()
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub fn encoded_len(&self) -> usize {
        debug_assert_eq!(self.encoded_len, self.inner.encoded_len());
        self.encoded_len
    }

    pub fn finish(self) -> Option<VectorBlock> {
        if !self.inner.info_offset.is_empty() {
            Some(VectorBlock(self.inner))
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

    pub fn encoded_len(&self) -> usize {
        self.0.encoded_len()
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
        let vector_payload_ptr = self.0.vector_payload.as_slice().as_ptr() as *const u8;
        // safety: correctly set the size of vector_payload
        let vector_payload_slice = unsafe {
            slice::from_raw_parts(
                vector_payload_ptr,
                self.0.vector_payload.len() * size_of::<f32>(),
            )
        };
        buf.put_slice(vector_payload_slice);
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

        let vector_payload_ptr = vector_payload.spare_capacity_mut().as_mut_ptr() as *mut u8;
        // safety: no data append to vector_payload, and correctly set the size of vector_payload
        let vector_payload_slice = unsafe {
            slice::from_raw_parts_mut(vector_payload_ptr, vector_item_count * size_of::<f32>())
        };
        buf.copy_to_slice(vector_payload_slice);
        // safety: have written correct amount of data
        unsafe {
            vector_payload.set_len(vector_item_count);
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
        (0..self.0.info_offset.len()).map(|i| (self.vec_ref(i), self.info(i)))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::util::iter_util::ZipEqDebug;

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
        let expected_encoded_len = builder.encoded_len();
        let block = builder.finish().unwrap();
        let mut encoded_block = Vec::new();
        block.encode(&mut encoded_block);
        assert_eq!(expected_encoded_len, encoded_block.len());
        let decoded_block = VectorBlock::decode(encoded_block.as_slice());
        assert_eq!(block, decoded_block);
        for ((expected_vec, expected_info), (actual_vec, actual_info)) in
            input.iter().zip_eq_debug(&block)
        {
            assert_eq!(expected_vec.to_ref(), actual_vec);
            assert_eq!(expected_info.iter().as_slice(), actual_info);
        }
    }

    #[test]
    fn test_empty_builder() {
        let builder = VectorBlockBuilder::new(VECTOR_DIM);
        assert!(builder.finish().is_none());
    }
}
